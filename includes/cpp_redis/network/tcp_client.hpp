#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <stdexcept>

#include "cpp_redis/network/io_service.hpp"
#include "cpp_redis/redis_error.hpp"

namespace cpp_redis {

namespace network {

//! tcp_client
//! async tcp client based on boost asio
class tcp_client {
public:
  //! ctor & dtor
  tcp_client(void);
  ~tcp_client(void);

  //! assignment operator & copy ctor
  tcp_client(const tcp_client&) = delete;
  tcp_client& operator=(const tcp_client&) = delete;

  //! returns whether the client is connected or not
  bool is_connected(void);

  //! handle connection & disconnection
  void connect(const std::string& host, unsigned int port);
  void disconnect(void);

  //! send data
  void send(const std::string& buffer);
  void send(const std::vector<char>& buffer);

  //! set receive callback
  //! called each time some data has been received
  //! Returns true if everything is ok, false otherwise (trigger disconnection)
  typedef std::function<bool(tcp_client&, const std::vector<char>& buffer)> receive_handler_t;
  void set_receive_handler(const receive_handler_t& handler);

  //! set disconnection callback
  //! called each time a disconnection has been detected
  typedef std::function<void(tcp_client&)> disconnection_handler_t;
  void set_disconnection_handler(const disconnection_handler_t& handler);

private:
  //! make boost asio async read and write operations
  void async_read(void);
  void async_write(void);

  //! io service callback
  void io_service_disconnection_handler(io_service&);

private:
  //! socket fd
  int m_fd;

  //! is connected
  std::atomic_bool m_is_connected;

  //! buffers
  static const unsigned int READ_SIZE = 2048;
  std::vector<char> m_read_buffer;
  std::vector<char> m_write_buffer;

  //! handlers
  receive_handler_t m_receive_handler;
  disconnection_handler_t m_disconnection_handler;

  //! thread safety
  std::mutex m_write_buffer_mutex;
  std::mutex m_receive_handler_mutex;
  std::mutex m_disconnection_handler_mutex;
};

} //! network

} //! cpp_redis
