#include <chainbase/chainbase.hpp>
#include <boost/array.hpp>

#include <iostream>

#include <sys/mman.h>

namespace chainbase {

   struct environment_check {
      environment_check() {
         memset( &compiler_version, 0, sizeof( compiler_version ) );
         memcpy( &compiler_version, __VERSION__, std::min<size_t>( strlen(__VERSION__), 256 ) );
#ifndef NDEBUG
         debug = true;
#endif
#ifdef __APPLE__
         apple = true;
#endif
#ifdef WIN32
         windows = true;
#endif
         boost_version = BOOST_VERSION;
      }
      friend bool operator == ( const environment_check& a, const environment_check& b ) {
         return std::make_tuple( a.compiler_version, a.debug, a.apple, a.windows, a.boost_version )
            ==  std::make_tuple( b.compiler_version, b.debug, b.apple, b.windows, b.boost_version );
      }
      friend bool operator != ( const environment_check& a, const environment_check& b ) {
         return !(a == b);
      }

      boost::array<char,256>  compiler_version;
      bool                    debug = false;
      bool                    apple = false;
      bool                    windows = false;
      uint32_t                boost_version;
   };

   database::database(const bfs::path& dir, open_flags flags, uint64_t shared_file_size, bool allow_dirty,
                      pinnable_mapped_file::map_mode db_map_mode, std::vector<std::string> hugepage_paths ) :
      _db_file(dir, flags & database::read_write, shared_file_size, allow_dirty, db_map_mode, hugepage_paths),
      _read_only(flags == database::read_only)
   {
      environment_check* env = nullptr;
      if(_read_only)
         env = _db_file.get_segment_manager()->find_no_lock< environment_check >( "environment" ).first;
      else
         env = _db_file.get_segment_manager()->find_or_construct< environment_check >( "environment" )();
      environment_check host_env = environment_check();
      if( *env != host_env ) {
         std::cerr << "database created by a different compiler, build, boost version, or operating system\n"
                     << "Environment differences (host vs database):"
                     << "\n Compiler Version: \n"
                     <<   "                   " << std::hex;
         for( uint32_t i = 0; i < 256; ++i ) {
            char b = *(host_env.compiler_version.data() + i);
            std::cerr << (uint16_t)b;
         }
         std::cerr << " \"";
         for( uint32_t i = 0; i < 256; ++i ) {
            char b = *(host_env.compiler_version.data() + i);
            if( !b ) break;
            std::cerr << b;
         }
         std::cerr << " \"";
         std::cerr << "\n                   vs\n"
                     <<   "                   ";
         for( uint32_t i = 0; i < 256; ++i ) {
            char b = *(env->compiler_version.data() + i);
            std::cerr << (uint16_t)b;
         }
         std::cerr << " \"";
         for( uint32_t i = 0; i < 256; ++i ) {
            char b = *(env->compiler_version.data() + i);
            if( !b ) break;
            std::cerr << b;
         }
         std::cerr << " \"" << std::dec;
         std::cerr << "\n Debug: " << host_env.debug << " vs " << env->debug
                     << "\n Apple: " << host_env.apple << " vs " << env->apple
                     << "\n Windows: " << host_env.windows << " vs " << env->windows
                     << "\n Boost Version: " << host_env.boost_version << " vs " << env->boost_version
                     << std::endl;

         BOOST_THROW_EXCEPTION( std::runtime_error( "database created by a different compiler, build, boost version, or operating system" ) );
      }
   }

   database::~database()
   {
      _index_list.clear();
      _index_map.clear();
   }

   void database::set_require_locking( bool enable_require_locking )
   {
#ifdef CHAINBASE_CHECK_LOCKING
      _enable_require_locking = enable_require_locking;
#endif
   }

#ifdef CHAINBASE_CHECK_LOCKING
   void database::require_lock_fail( const char* method, const char* lock_type, const char* tname )const
   {
      std::string err_msg = "database::" + std::string( method ) + " require_" + std::string( lock_type ) + "_lock() failed on type " + std::string( tname );
      std::cerr << err_msg << std::endl;
      BOOST_THROW_EXCEPTION( std::runtime_error( err_msg ) );
   }
#endif

   void database::undo()
   {
      for( auto& item : _index_list )
      {
         item->undo();
      }
   }

   void database::squash()
   {
      for( auto& item : _index_list )
      {
         item->squash();
      }
   }

   void database::commit( int64_t revision )
   {
      for( auto& item : _index_list )
      {
         item->commit( revision );
      }
   }

   void database::undo_all()
   {
      for( auto& item : _index_list )
      {
         item->undo_all();
      }
   }

   database::session database::start_undo_session( bool enabled )
   {
      if( enabled ) {
         vector< std::unique_ptr<abstract_session> > _sub_sessions;
         _sub_sessions.reserve( _index_list.size() );
         for( auto& item : _index_list ) {
            _sub_sessions.push_back( item->start_undo_session( enabled ) );
         }
         return session( std::move( _sub_sessions ) );
      } else {
         return session();
      }
   }

}  // namespace chainbase
