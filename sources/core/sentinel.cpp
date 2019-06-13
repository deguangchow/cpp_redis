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

#include <cpp_redis/core/sentinel.hpp>
#include <cpp_redis/misc/error.hpp>
#include <cpp_redis/misc/logger.hpp>
#include <cpp_redis/network/redis_connection.hpp>

namespace cpp_redis {

#ifndef __CPP_REDIS_USE_CUSTOM_TCP_CLIENT
sentinel::sentinel(void)
: m_nRunningCallbacks_a(0) {
  __CPP_REDIS_LOG(debug, "cpp_redis::sentinel created");
}
#endif /* __CPP_REDIS_USE_CUSTOM_TCP_CLIENT */

sentinel::sentinel(const std::shared_ptr<network::tcp_client_iface>& ptrTcpClient)
: m_redisConnection(ptrTcpClient)
, m_nRunningCallbacks_a(0) {
  __CPP_REDIS_LOG(debug, "cpp_redis::sentinel created");
}

sentinel::~sentinel(void) {
  m_vctSentinels.clear();
  if (m_redisConnection.is_connected())
    m_redisConnection.disconnect(true);
  __CPP_REDIS_LOG(debug, "cpp_redis::sentinel destroyed");
}

sentinel&
sentinel::add_sentinel(const std::string& sHost, std::size_t nPort, std::uint32_t uTimeoutMsecs) {
  m_vctSentinels.push_back({sHost, nPort, uTimeoutMsecs});
  return *this;
}

void
sentinel::clear_sentinels() {
  m_vctSentinels.clear();
}

bool
sentinel::get_master_addr_by_name(const std::string& sSentinelName, std::string& sHost, std::size_t& nPort,
    bool bAutoConnect) {
  //! reset connection settings
  sHost = "";
  nPort = 0;

  //! we must have some sentinels to connect to if we are in autoconnect mode
  if (bAutoConnect && m_vctSentinels.size() == 0) {
    throw redis_error("No sentinels available. Call add_sentinel() before get_master_addr_by_name()");
  }

  //! if we are not connected and we are not in autoconnect mode, we can't go further in the process
  if (!bAutoConnect && !is_connected()) {
    throw redis_error("No sentinel connected. Call connect() first or enable autoconnect.");
  }

  if (bAutoConnect) {
    try {
      //! Will round robin all attached sentinels until it finds one that is online.
      connect_sentinel(nullptr);
    }
    catch (const redis_error&) {
    }

    //! we failed to connect
    if (!is_connected()) {
      return false;
    }
  }

  //! By now we have a connection to a redis sentinel.
  //! Ask it who the master is.
  send({"SENTINEL", "get-master-addr-by-name", sSentinelName}, [&](cpp_redis::reply& reply) {
    if (reply.is_array()) {
      auto arr  = reply.as_array();
      sHost     = arr[0].as_string();
      nPort     = std::stoi(arr[1].as_string(), nullptr, 10);
    }
  });
  sync_commit();

  //! We always close any open connection in auto connect mode
  //! since the sentinel may not be around next time we ask who the master is.
  if (bAutoConnect) {
    disconnect(true);
  }

  return nPort != 0;
}

void
sentinel::connect_sentinel(const sentinel_disconnect_handler_t& handlerSentinelDisconnect) {
  if (m_vctSentinels.size() == 0) {
    throw redis_error("No sentinels available. Call add_sentinel() before connect_sentinel()");
  }

  auto const& handlerDisconnect = std::bind(&sentinel::connection_disconnect_handler, this, std::placeholders::_1);
  auto const& handlerReceive    = std::bind(&sentinel::connection_receive_handler, this, std::placeholders::_1,
      std::placeholders::_2);

  //! Now try to connect to first sentinel
  auto pos              = m_vctSentinels.begin();
  bool bNotConnected    = true;

  while (bNotConnected && pos != m_vctSentinels.end()) {
    try {
      __CPP_REDIS_LOG(debug, std::string("cpp_redis::sentinel attempting to connect to host ") + pos->get_host());
      m_redisConnection.connect(pos->get_host(), pos->get_port(), handlerDisconnect, handlerReceive,
          pos->get_timeout_msecs());
    }
    catch (const redis_error&) {
      __CPP_REDIS_LOG(info, std::string("cpp_redis::sentinel unable to connect to sentinel host ") + pos->get_host());
    }

    if (is_connected()) {
      bNotConnected = false;
      __CPP_REDIS_LOG(info, std::string("cpp_redis::sentinel connected ok to host ") + pos->get_host());
    } else {
      //! Make sure its closed.
      disconnect(true);
      //! Could not connect.  Try the next sentinel.
      ++pos;
    }
  }

  if (bNotConnected) {
    throw redis_error("Unable to connect to any sentinels");
  }

  m_handlerDisconnect = handlerSentinelDisconnect;
}

void
sentinel::connect(const std::string& sHost, std::size_t nPort,
  const sentinel_disconnect_handler_t& handlerDisconnect,
  std::uint32_t uTimeoutMsecs) {
  __CPP_REDIS_LOG(debug, "cpp_redis::sentinel attempts to connect");

  auto handlerClientDisconnect  = std::bind(&sentinel::connection_disconnect_handler, this, std::placeholders::_1);
  auto handlerReceive           = std::bind(&sentinel::connection_receive_handler, this, std::placeholders::_1,
      std::placeholders::_2);

  m_redisConnection.connect(sHost, nPort, handlerClientDisconnect, handlerReceive, uTimeoutMsecs);

  __CPP_REDIS_LOG(info, "cpp_redis::sentinel connected");

  m_handlerDisconnect = handlerDisconnect;
}

void
sentinel::connection_receive_handler(network::redis_connection&, reply& reply) {
  reply_callback_t callback = nullptr;

  __CPP_REDIS_LOG(info, "cpp_redis::sentinel received reply");
  {
    std::lock_guard<std::mutex> lock(m_mtxCallbacks);
    m_nRunningCallbacks_a += 1;

    if (m_queCallbacks.size()) {
      callback = m_queCallbacks.front();
      m_queCallbacks.pop();
    }
  }

  if (callback) {
    __CPP_REDIS_LOG(debug, "cpp_redis::sentinel executes reply callback");
    callback(reply);
  }

  {
    std::lock_guard<std::mutex> lock(m_mtxCallbacks);
    m_nRunningCallbacks_a -= 1;
    m_cvSync.notify_all();
  }
}

void
sentinel::clear_callbacks(void) {
  std::lock_guard<std::mutex> lock(m_mtxCallbacks);

  std::queue<reply_callback_t> empty;
  std::swap(m_queCallbacks, empty);

  m_cvSync.notify_all();
}

void
sentinel::call_disconnect_handler(void) {
  if (m_handlerDisconnect) {
    __CPP_REDIS_LOG(info, "cpp_redis::sentinel calls disconnect handler");
    m_handlerDisconnect(*this);
  }
}

void
sentinel::connection_disconnect_handler(network::redis_connection&) {
  __CPP_REDIS_LOG(warn, "cpp_redis::sentinel has been disconnected");
  clear_callbacks();
  call_disconnect_handler();
}

void
sentinel::disconnect(bool bWaitForRemoval) {
  __CPP_REDIS_LOG(debug, "cpp_redis::sentinel attempts to disconnect");
  m_redisConnection.disconnect(bWaitForRemoval);
  __CPP_REDIS_LOG(info, "cpp_redis::sentinel disconnected");
}

bool
sentinel::is_connected(void) {
  return m_redisConnection.is_connected();
}

const std::vector<sentinel::sentinel_def>&
sentinel::get_sentinels(void) const {
  return m_vctSentinels;
}

std::vector<sentinel::sentinel_def>&
sentinel::get_sentinels(void) {
  return m_vctSentinels;
}

sentinel&
sentinel::send(const std::vector<std::string>& sRedisCmd, const reply_callback_t& callback) {
  std::lock_guard<std::mutex> ulockCallback(m_mtxCallbacks);

  __CPP_REDIS_LOG(info, "cpp_redis::sentinel attempts to store new command in the send buffer");
  m_redisConnection.send(sRedisCmd);
  m_queCallbacks.push(callback);
  __CPP_REDIS_LOG(info, "cpp_redis::sentinel stored new command in the send buffer");

  return *this;
}

//! commit pipelined transaction
sentinel&
sentinel::commit(void) {
  try_commit();

  return *this;
}

sentinel&
sentinel::sync_commit() {
  try_commit();
  std::unique_lock<std::mutex> ulockCallback(m_mtxCallbacks);
  __CPP_REDIS_LOG(debug, "cpp_redis::sentinel waiting for callbacks to complete");
  m_cvSync.wait(ulockCallback, [=] { return m_nRunningCallbacks_a == 0 && m_queCallbacks.empty(); });
  __CPP_REDIS_LOG(debug, "cpp_redis::sentinel finished waiting for callback completion");
  return *this;
}

void
sentinel::try_commit(void) {
  try {
    __CPP_REDIS_LOG(debug, "cpp_redis::sentinel attempts to send pipelined commands");
    m_redisConnection.commit();
    __CPP_REDIS_LOG(info, "cpp_redis::sentinel sent pipelined commands");
  }
  catch (const cpp_redis::redis_error& e) {
    __CPP_REDIS_LOG(error, "cpp_redis::sentinel could not send pipelined commands");
    clear_callbacks();
    throw e;
  }
}

sentinel&
sentinel::ping(const reply_callback_t& reply_callback) {
  send({"PING"}, reply_callback);
  return *this;
}

sentinel&
sentinel::masters(const reply_callback_t& reply_callback) {
  send({"SENTINEL", "MASTERS"}, reply_callback);
  return *this;
}

sentinel&
sentinel::master(const std::string& name, const reply_callback_t& reply_callback) {
  send({"SENTINEL", "MASTER", name}, reply_callback);
  return *this;
}

sentinel&
sentinel::slaves(const std::string& name, const reply_callback_t& reply_callback) {
  send({"SENTINEL", "SLAVES", name}, reply_callback);
  return *this;
}

sentinel&
sentinel::sentinels(const std::string& name, const reply_callback_t& reply_callback) {
  send({"SENTINEL", "SENTINELS", name}, reply_callback);
  return *this;
}

sentinel&
sentinel::ckquorum(const std::string& name, const reply_callback_t& reply_callback) {
  send({"SENTINEL", "CKQUORUM", name}, reply_callback);
  return *this;
}

sentinel&
sentinel::failover(const std::string& name, const reply_callback_t& reply_callback) {
  send({"SENTINEL", "FAILOVER", name}, reply_callback);
  return *this;
}

sentinel&
sentinel::reset(const std::string& pattern, const reply_callback_t& reply_callback) {
  send({"SENTINEL", "RESET", pattern}, reply_callback);
  return *this;
}

sentinel&
sentinel::flushconfig(const reply_callback_t& reply_callback) {
  send({"SENTINEL", "FLUSHCONFIG"}, reply_callback);
  return *this;
}

sentinel&
sentinel::monitor(const std::string& name, const std::string& ip, std::size_t port, std::size_t quorum,
  const reply_callback_t& reply_callback) {
  send({"SENTINEL", "MONITOR", name, ip, std::to_string(port), std::to_string(quorum)}, reply_callback);
  return *this;
}

sentinel&
sentinel::remove(const std::string& name, const reply_callback_t& reply_callback) {
  send({"SENTINEL", "REMOVE", name}, reply_callback);
  return *this;
}

sentinel&
sentinel::set(const std::string& name, const std::string& option, const std::string& value,
  const reply_callback_t& reply_callback) {
  send({"SENTINEL", "SET", name, option, value}, reply_callback);
  return *this;
}

} //namespace cpp_redis

