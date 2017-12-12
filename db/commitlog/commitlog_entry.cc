/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "counters.hh"
#include "commitlog_entry.hh"
#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/mutation.dist.hh"
#include "idl/commitlog.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/commitlog.dist.impl.hh"

template<typename Output>
void commitlog_entry_writer::serialize(Output& out) const {
    print("commitlog_entry_writer::serialize -> convert out=%s to wr=ser::writer_of_commitlog_entry<Output> type\n",typeid(out).name());
    [this, wr = ser::writer_of_commitlog_entry<Output>(out)] () mutable {
        if (_with_schema) {
            print("commitlog_entry_writer::serialize -> _with_schema=true,call wr.write_mapping, which returns after_commitlog_entry__mapping\n");
            return std::move(wr).write_mapping(_schema->get_column_mapping());
        } else {
            print("commitlog_entry_writer::serialize -> _with_schema=false,call wr.skip_mapping, which returns after_commitlog_entry__mapping\n");
            return std::move(wr).skip_mapping();
        }
        print("commitlog_entry_writer::serialize -> after write_mapping or skip_mapping, call after_commitlog_entry__mapping::write_mutation, \
\033[34mwhich inside call the global serialize(), then call after_commitlog_entry__mapping::end_commitlog_entry()\033[0m\n\n");
    }().write_mutation(_mutation).end_commitlog_entry();
}

void commitlog_entry_writer::compute_size() {
    seastar::measuring_output_stream ms;
    serialize(ms);
    _size = ms.size();
    print("commitlog_entry_writer::compute_size -> called by set_with_schema, create a local measuring_output_stream and assign its serialized size to this._size=%d\n",_size);
}

void commitlog_entry_writer::write(data_output& out) const {
    auto p = out.reserve(size());
    print("commitlog_entry_writer::write -> create class=simple_memory_output_stream str(data_output.reserve,size()=%d), and serialize(str). \
            \033[34mHere pass data_output._ptr, which points to %p of segment._buffer, to a simple_memory_out_stream object `str`. when `str` call write(mutation data), \
it actually copy the byte_stream mutation data to segment's_circular_buffer\033[0m\n\n",size(),p);
    seastar::simple_output_stream str(p, size());
    serialize(str);
}

commitlog_entry_reader::commitlog_entry_reader(const temporary_buffer<char>& buffer)
    : _ce([&] {
    seastar::simple_input_stream in(buffer.get(), buffer.size());
    return ser::deserialize(in, boost::type<commitlog_entry>());
}())
{
}
