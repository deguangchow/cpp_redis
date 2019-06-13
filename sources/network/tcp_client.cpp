// MIT License
//
// Copyright (c) 2016-2017 Simon Ninon <simon.ninon@gmail.com>
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

#include <cpp_redis/misc/macro.hpp>
#include <cpp_redis/network/tcp_client.hpp>

namespace cpp_redis {

namespace network {

void
tcp_client::connect(const std::string& sAddr, std::uint32_t uPort, std::uint32_t uTimeoutMsecs) {
  m_tcpClient.connect(sAddr, uPort, uTimeoutMsecs);
}


void
tcp_client::disconnect(bool bWaitForRemoval) {
  m_tcpClient.disconnect(bWaitForRemoval);
}

bool
tcp_client::is_connected(void) const {
  return m_tcpClient.is_connected();
}

void
tcp_client::set_nb_workers(std::size_t nNbThreads) {
  m_tcpClient.get_io_service()->set_nb_workers(nNbThreads);
}

void
tcp_client::async_read(read_request& requestRead) {
  auto callbackAsyncRead = std::move(requestRead.callbackAsyncRead);

  m_tcpClient.async_read({requestRead.nSizeToRead, [=](tacopie::tcp_client::read_result& resultRead) {
                         if (!callbackAsyncRead) {
                           return;
                         }

                         read_result resultReadConverted = {resultRead.success, std::move(resultRead.buffer)};
                         callbackAsyncRead(resultReadConverted);
                       }});
}

void
tcp_client::async_write(write_request& requestWrite) {
  auto callbackAsyncWrite = std::move(requestWrite.callbackAsyncWrite);

  m_tcpClient.async_write({std::move(requestWrite.vctBuffer), [=](tacopie::tcp_client::write_result& resultWrite) {
                          if (!callbackAsyncWrite) {
                            return;
                          }

                          write_result resultWriteConverted = {resultWrite.success, resultWrite.size};
                          callbackAsyncWrite(resultWriteConverted);
                        }});
}

void
tcp_client::set_on_disconnection_handler(const disconnection_handler_t& handlerDisconnection) {
  m_tcpClient.set_on_disconnection_handler(handlerDisconnection);
}

void
set_default_nb_workers(std::size_t nNbThreads) {
  tacopie::get_default_io_service()->set_nb_workers(__CPP_REDIS_LENGTH(nNbThreads));
}

} // namespace network

} // namespace cpp_redis
