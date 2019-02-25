#include <chainbase/pinnable_mapped_file.hpp>
#include <boost/interprocess/managed_external_buffer.hpp>
#include <boost/interprocess/anonymous_shared_memory.hpp>
#include <boost/asio/signal_set.hpp>
#include <iostream>

#ifdef __linux__
#include <sys/vfs.h>
#include <linux/magic.h>
#endif

namespace chainbase {

pinnable_mapped_file::pinnable_mapped_file(const bfs::path& dir, bool writable, uint64_t shared_file_size, bool allow_dirty,
                                          map_mode mode, std::vector<std::string> hugepage_paths) :
   _data_file_path(bfs::absolute(dir/"shared_memory.bin")),
   _database_name(dir.filename().string()),
   _writable(writable)
{
   if(shared_file_size % _db_size_multiple_requirement)
      BOOST_THROW_EXCEPTION(std::runtime_error("Database must be mulitple of " + std::to_string(_db_size_multiple_requirement) + " bytes"));
#ifndef __linux__
   if(hugepage_paths.size())
      BOOST_THROW_EXCEPTION(std::runtime_error("Hugepage support is a linux only feature"));
#endif
   if(hugepage_paths.size() && mode != locked)
      BOOST_THROW_EXCEPTION(std::runtime_error("Locked mode is required for hugepage usage"));
#ifdef _WIN32
   if(mode == locked)
      BOOST_THROW_EXCEPTION(std::runtime_error("Locked mode not supported on win32"));
#endif

   if(!_writable && !bfs::exists(_data_file_path))
      BOOST_THROW_EXCEPTION(std::runtime_error("database file not found at " + _data_file_path.native()));
   bfs::create_directories(dir);

   if(bfs::exists(_data_file_path)) {
      if(_writable) {
         auto existing_file_size = bfs::file_size(_data_file_path);
         if(shared_file_size > existing_file_size) {
            if(!bip::managed_mapped_file::grow(_data_file_path.generic_string().c_str(), shared_file_size - existing_file_size))
               BOOST_THROW_EXCEPTION(std::runtime_error( "could not grow database file to requested size."));
         }

         _mapped_file = std::make_unique<bip::managed_mapped_file>(bip::open_only,
                                                                   _data_file_path.generic_string().c_str());
      } else {
         _mapped_file = std::make_unique<bip::managed_mapped_file>(bip::open_read_only,
                                                                   _data_file_path.generic_string().c_str());
      }
   }
   else {
         _mapped_file = std::make_unique<bip::managed_mapped_file>(bip::create_only,
                                                                   _data_file_path.generic_string().c_str(),
                                                                   shared_file_size, nullptr, _db_permissions);
   }

   if(_writable) {
      boost::system::error_code ec;
      bfs::remove(bfs::absolute(dir/"shared_memory.meta"), ec);
   }

   bool* db_is_dirty  = nullptr;

   if(_writable)
      db_is_dirty = _mapped_file->find_or_construct<bool>(_db_dirty_flag_string)(false);
   else
      db_is_dirty = _mapped_file->find_no_lock<bool>(_db_dirty_flag_string).first;

   if(db_is_dirty == nullptr)
      BOOST_THROW_EXCEPTION(std::runtime_error( "could not find dirty flag in shared memory"));

   if(!allow_dirty && *db_is_dirty)
      throw std::runtime_error("database dirty flag set");

   if(_writable) {
      _mapped_file_lock = bip::file_lock(_data_file_path.generic_string().c_str());
      if(!_mapped_file_lock.try_lock())
         BOOST_THROW_EXCEPTION(std::runtime_error("could not gain write access to the shared memory file"));

      *db_is_dirty = true;
      msync_boost_mapped_file();
   }

   if(mode == mapped) {
      _segment_manager = _mapped_file->get_segment_manager();
   }
   else {
      boost::asio::io_service sig_ios;
      boost::asio::signal_set sig_set(sig_ios, SIGINT, SIGTERM, SIGPIPE);
      sig_set.async_wait([](const boost::system::error_code&, int) {
         BOOST_THROW_EXCEPTION(std::runtime_error("Database load aborted"));
      });

      try {
         if(mode == heap)
            _mapped_region = bip::mapped_region(bip::anonymous_shared_memory(shared_file_size));
         else
            _mapped_region = get_huge_region(hugepage_paths);

         load_database_file(sig_ios);

         if(mode == locked) {
#ifndef _WIN32
            if(mlock(_mapped_region.get_address(), _mapped_region.get_size()))
               BOOST_THROW_EXCEPTION(std::runtime_error("Failed to mlock database \"" + _database_name + "\""));
            std::cerr << "CHAINBASE: Database \"" << _database_name << "\" has been successfully locked in memory" << std::endl;
#endif
         }
      }
      catch(...) {
         *db_is_dirty = false;
         msync_boost_mapped_file();
         throw;
      }

      /* a managed_mapped_file contains a header used for "atomic" creation that steals some bytes
         from the useable space, compute this using a non-private api to figure out where the segment_manager
         exists in the mapped file */
      size_t segment_offset = _mapped_file->get_size() - _mapped_file->get_segment_manager()->get_size();
      _mapped_file.reset();
      _segment_manager = reinterpret_cast<segment_manager*>((char*)_mapped_region.get_address()+segment_offset);
   }
}

bip::mapped_region pinnable_mapped_file::get_huge_region(const std::vector<std::string>& huge_paths) {
   std::map<unsigned, std::string> page_size_to_paths;
   const auto mapped_file_size = _mapped_file->get_size();

#ifdef __linux__
   for(const std::string& p : huge_paths) {
      struct statfs fs;
      if(statfs(p.c_str(), &fs))
         BOOST_THROW_EXCEPTION(std::runtime_error(std::string("Could not statfs() path ") + p));
      if(fs.f_type != HUGETLBFS_MAGIC)
         BOOST_THROW_EXCEPTION(std::runtime_error(p + std::string(" does not look like a hugepagefs mount")));
      page_size_to_paths[fs.f_bsize] = p;
   }
   for(auto it = page_size_to_paths.rbegin(); it != page_size_to_paths.rend(); ++it) {
      if(mapped_file_size % it->first == 0) {
         bfs::path hugepath = bfs::unique_path(bfs::path(it->second + "/%%%%%%%%%%%%%%%%%%%%%%%%%%"));
         int fd = creat(hugepath.string().c_str(), _db_permissions.get_permissions());
         if(fd < 0)
            BOOST_THROW_EXCEPTION(std::runtime_error(std::string("Could not open hugepage file in ") + it->second + ": " + std::string(strerror(errno))));
         if(ftruncate(fd, mapped_file_size))
            BOOST_THROW_EXCEPTION(std::runtime_error(std::string("Failed to grow hugepage file to specified size")));
         close(fd);
         bip::file_mapping filemap(hugepath.generic_string().c_str(), _writable ? bip::read_write : bip::read_only);
         bfs::remove(hugepath);
         std::cerr << "CHAINBASE: Database \"" << _database_name << "\" using " << it->first << " byte pages" << std::endl;
         return bip::mapped_region(filemap, _writable ? bip::read_write : bip::read_only);
      }
   }
#endif

   std::cerr << "CHAINBASE: Database \"" << _database_name << "\" not using huge pages" << std::endl;
   return bip::mapped_region(bip::anonymous_shared_memory(mapped_file_size));
}

void pinnable_mapped_file::load_database_file(boost::asio::io_service& sig_ios) {
   std::cerr << "CHAINBASE: Preloading \"" << _database_name << "\" database file, this could take a moment..." << std::endl;
   char* const src = (char*)_mapped_file->get_address();
   char* const dst = (char*)_mapped_region.get_address();
   size_t offset = 0;
   time_t t = time(nullptr);
   while(offset != _mapped_file->get_size()) {
      memcpy(dst+offset, src+offset, _db_size_multiple_requirement);
      offset += _db_size_multiple_requirement;

      if(time(nullptr) != t) {
         t = time(nullptr);
         std::cerr << "              " << offset/(_mapped_region.get_size()/100) << "% complete..." << std::endl;
      }
      sig_ios.poll();
   }
   std::cerr << "           Complete" << std::endl;
}

bool pinnable_mapped_file::all_zeros(char* data, size_t sz) {
   uint64_t* p = (uint64_t*)data;
   uint64_t* end = p+sz/sizeof(uint64_t);
   while(p != end) {
      if(*p++ != 0)
         return false;
   }
   return true;
}

void pinnable_mapped_file::save_database_file() {
   bip::file_mapping filemap(_data_file_path.generic_string().c_str(), bip::read_write);
   bip::mapped_region region(filemap, bip::read_write);

   std::cerr << "CHAINBASE: Writing \"" << _database_name << "\" database file, this could take a moment..." << std::endl;
   char* src = (char*)_mapped_region.get_address();
   char* dst = (char*)region.get_address();
   size_t offset = 0;
   time_t t = time(nullptr);
   while(offset != region.get_size()) {
      if(!all_zeros(src+offset, _db_size_multiple_requirement))
         memcpy(dst+offset, src+offset, _db_size_multiple_requirement);
      offset += _db_size_multiple_requirement;

      if(time(nullptr) != t) {
         t = time(nullptr);
         std::cerr << "              " << offset/(region.get_size()/100) << "% complete..." << std::endl;
      }
   }
   std::cerr << "           Syncing buffers..." << std::endl;
   if(region.flush(0, region.get_size(), false) == false)
      std::cerr << "CHAINBASE: ERROR: syncing buffers failed" << std::endl;
   std::cerr << "           Complete" << std::endl;
}

void pinnable_mapped_file::finialize_database_file(bool* dirty) {
   bip::file_mapping filemap(_data_file_path.generic_string().c_str(), bip::read_write);
   bip::mapped_region region(filemap, bip::read_write);

   uintptr_t offset = (char*)dirty-(char*)_mapped_region.get_address();

   memcpy((char*)(region.get_address())+offset, (char*)(_mapped_region.get_address())+offset, sizeof(bool));
   if(region.flush(0, region.get_size(), false) == false)
      std::cerr << "CHAINBASE: ERROR: syncing dirty bit failed" << std::endl;
}

pinnable_mapped_file::~pinnable_mapped_file() {
   const bool is_heap_or_locked = _mapped_region.get_address();
   if(_writable) {
      if(is_heap_or_locked)
         save_database_file();
      bool* dirty = _segment_manager->find<bool>(_db_dirty_flag_string).first;
      *dirty = false;
      if(is_heap_or_locked)
         finialize_database_file(dirty);
      else
         msync_boost_mapped_file();
#ifdef _WIN32
      std::cerr << "Warning: chainbase cannot ensure safe database sync on win32" << std::endl;
#endif
   }
}

void pinnable_mapped_file::msync_boost_mapped_file() {
#ifndef _WIN32
   if(msync(_mapped_file->get_address(), _mapped_file->get_size(), MS_SYNC))
      perror("Failed to msync DB file");
#endif
}

std::istream& operator>>(std::istream& in, pinnable_mapped_file::map_mode& runtime) {
   std::string s;
   in >> s;
   if (s == "mapped")
      runtime = pinnable_mapped_file::map_mode::mapped;
   else if (s == "heap")
      runtime = pinnable_mapped_file::map_mode::heap;
   else if (s == "locked")
      runtime = pinnable_mapped_file::map_mode::locked;
   else
      in.setstate(std::ios_base::failbit);
   return in;
}

std::ostream& operator<<(std::ostream& osm, pinnable_mapped_file::map_mode m) {
   if(m == pinnable_mapped_file::map_mode::mapped)
      osm << "mapped";
   else if (m == pinnable_mapped_file::map_mode::heap)
      osm << "heap";
   else if (pinnable_mapped_file::map_mode::locked)
      osm << "locked";

   return osm;
}

}
