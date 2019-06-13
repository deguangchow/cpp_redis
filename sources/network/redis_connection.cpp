// The MIT License (MIT)
//
// Copyright (c) 2015-2017 Simon Ninon <simon.ninon@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <cpp_redis/misc/error.hpp>
#include <cpp_redis/misc/logger.hpp>
#include <cpp_redis/network/redis_connection.hpp>

#ifndef __CPP_REDIS_USE_CUSTOM_TCP_CLIENT
#include <cpp_redis/network/tcp_client.hpp>
#endif /* __CPP_REDIS_USE_CUSTOM_TCP_CLIENT */

namespace cpp_redis {

namespace network {

#ifndef __CPP_REDIS_USE_CUSTOM_TCP_CLIENT
redis_connection::redis_connection(void)
: redis_connection(std::make_shared<tcp_client>()) {
}
#endif /* __CPP_REDIS_USE_CUSTOM_TCP_CLIENT */

redis_connection::redis_connection(const std::shared_ptr<tcp_client_iface>& ptrTcpClient)
: m_ptrTcpClient(ptrTcpClient)
, m_callbackReply(nullptr)
, m_handlerDisconnection(nullptr) {
  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection created");
}

redis_connection::~redis_connection(void) {
  m_ptrTcpClient->disconnect(true);
  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection destroyed");
}

void
redis_connection::connect(const std::string& sHost, std::size_t uPort,
  const disconnection_handler_t& handlerClientDisconnection,
  const reply_callback_t& callbackClientReply,
  std::uint32_t uTimeoutMsecs) {
  try {
    __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection attempts to connect");

    //! connect client
    m_ptrTcpClient->connect(sHost, (uint32_t) uPort, uTimeoutMsecs);
    m_ptrTcpClient->set_on_disconnection_handler(std::bind(&redis_connection::tcp_client_disconnection_handler, this));

    //! start to read asynchronously
    tcp_client_iface::read_request requestRead = {__CPP_REDIS_READ_SIZE,
        std::bind(&redis_connection::tcp_client_receive_handler, this, std::placeholders::_1)};
    m_ptrTcpClient->async_read(requestRead);

    __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection connected");
  }
  catch (const std::exception& e) {
    __CPP_REDIS_LOG(error, std::string("cpp_redis::network::redis_connection ") + e.what());
    throw redis_error(e.what());
  }

  m_callbackReply        = callbackClientReply;
  m_handlerDisconnection = handlerClientDisconnection;
}

void
redis_connection::disconnect(bool wait_for_removal) {
  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection attempts to disconnect");

  //! close connection
  m_ptrTcpClient->disconnect(wait_for_removal);

  //! clear buffer
  m_sBuffer.clear();
  //! clear builder
  m_builderReply.reset();

  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection disconnected");
}

bool
redis_connection::is_connected(void) const {
  return m_ptrTcpClient->is_connected();
}

std::string
redis_connection::build_command(const std::vector<std::string>& vctRedisCmd) {
  std::string sCmd = "*" + std::to_string(vctRedisCmd.size()) + "\r\n";

  for (const auto& sCmdPart : vctRedisCmd)
    sCmd += "$" + std::to_string(sCmdPart.length()) + "\r\n" + sCmdPart + "\r\n";

  return sCmd;
}

redis_connection&
redis_connection::send(const std::vector<std::string>& vctRedisCmd) {
  std::lock_guard<std::mutex> lock(m_mtxBuffer);

  m_sBuffer += build_command(vctRedisCmd);
  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection stored new command in the send buffer");

  return *this;
}

//! commit pipelined transaction
redis_connection&
redis_connection::commit(void) {
  std::lock_guard<std::mutex> lock(m_mtxBuffer);

  //! ensure buffer is cleared
  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection attempts to send pipelined commands");
  std::string sBuffer = std::move(m_sBuffer);

  try {
    tcp_client_iface::write_request requestWrite = {std::vector<char>{sBuffer.begin(), sBuffer.end()}, nullptr};
    m_ptrTcpClient->async_write(requestWrite);
  }
  catch (const std::exception& e) {
    __CPP_REDIS_LOG(error, std::string("cpp_redis::network::redis_connection ") + e.what());
    throw redis_error(e.what());
  }

  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection sent pipelined commands");

  return *this;
}

void
redis_connection::call_disconnection_handler(void) {
  if (m_handlerDisconnection) {
    __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection calls disconnection handler");
    m_handlerDisconnection(*this);
  }
}

void
redis_connection::tcp_client_receive_handler(const tcp_client_iface::read_result& resultRead) {
  if (!resultRead.bSuccess) { return; }

  try {
    __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection receives packet, attempts to build reply");
    m_builderReply << std::string(resultRead.vctBuffer.begin(), resultRead.vctBuffer.end());
  }
  catch (const redis_error&) {
    __CPP_REDIS_LOG(error, "cpp_redis::network::redis_connection could not build reply (invalid format),"
        " disconnecting");
    call_disconnection_handler();
    return;
  }

  while (m_builderReply.reply_available()) {
    __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection reply fully built");

    auto reply = m_builderReply.get_front();
    m_builderReply.pop_front();

    if (m_callbackReply) {
      __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection executes reply callback");
      m_callbackReply(*this, reply);
    }
  }

  try {
    tcp_client_iface::read_request requestRead = {__CPP_REDIS_READ_SIZE,
        std::bind(&redis_connection::tcp_client_receive_handler, this, std::placeholders::_1)};
    m_ptrTcpClient->async_read(requestRead);
  }
  catch (const std::exception&) {
    //! Client disconnected in the meantime
  }
}

void
redis_connection::tcp_client_disconnection_handler(void) {
  __CPP_REDIS_LOG(debug, "cpp_redis::network::redis_connection has been disconnected");
  //! clear buffer
  m_sBuffer.clear();
  //! clear builder
  m_builderReply.reset();
  //! call disconnection handler
  call_disconnection_handler();
}

} //namespace network

} // namespace cpp_redis
