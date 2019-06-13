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

#include <cpp_redis/builders/array_builder.hpp>
#include <cpp_redis/builders/builders_factory.hpp>
#include <cpp_redis/misc/error.hpp>
#include <cpp_redis/misc/logger.hpp>

namespace cpp_redis {

namespace builders {

array_builder::array_builder(void)
: m_ptrCurrentBuilder(nullptr)
, m_bReplyReady(false)
, m_reply(std::vector<reply>{}) {}

bool
array_builder::fetch_array_size(std::string& sBuffer) {
  if (m_intBuilder.reply_ready())
    return true;

  m_intBuilder << sBuffer;
  if (!m_intBuilder.reply_ready())
    return false;

  int64_t nSize = m_intBuilder.get_integer();
  if (nSize < 0) {
    m_reply.set();
    m_bReplyReady = true;
  } else if (nSize == 0) {
    m_bReplyReady = true;
  }

  m_uArraySize = nSize;

  return true;
}

bool
array_builder::build_row(std::string& sBuffer) {
  if (!m_ptrCurrentBuilder) {
    m_ptrCurrentBuilder = create_builder(sBuffer.front());
    sBuffer.erase(0, 1);
  }

  *m_ptrCurrentBuilder << sBuffer;
  if (!m_ptrCurrentBuilder->reply_ready())
    return false;

  m_reply << m_ptrCurrentBuilder->get_reply();
  m_ptrCurrentBuilder = nullptr;

  if (m_reply.as_array().size() == m_uArraySize)
    m_bReplyReady = true;

  return true;
}

builder_iface&
array_builder::operator<<(std::string& sBuffer) {
  if (m_bReplyReady)
    return *this;

  if (!fetch_array_size(sBuffer))
    return *this;

  while (sBuffer.size() && !m_bReplyReady)
    if (!build_row(sBuffer))
      return *this;

  return *this;
}

bool
array_builder::reply_ready(void) const {
  return m_bReplyReady;
}

reply
array_builder::get_reply(void) const {
  return reply{m_reply};
}

} // namespace builders

} // namespace cpp_redis
