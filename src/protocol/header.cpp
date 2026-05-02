#include "mkmq/protocol/header.hpp"

namespace mkmq::protocol {

RequestHeader parse_request_header(WireReader& reader) {
    const std::int16_t api_id_value = reader.read_int16();
    const std::int16_t version = reader.read_int16();
    const ApiKey key = api_key_from_id(api_id_value);
    const std::int16_t header_version = request_header_version(key, version);

    RequestHeader header;
    header.api_key = key;
    header.api_version = version;
    header.header_version = header_version;
    header.correlation_id = reader.read_int32();
    header.client_id = reader.read_nullable_string().value_or("");
    if (header_version >= 2) {
        reader.skip_tagged_fields();
    }
    return header;
}

void write_response_header(WireWriter& writer, const RequestHeader& request) {
    writer.write_int32(request.correlation_id);
    if (response_header_version(request.api_key, request.api_version) >= 1) {
        writer.write_empty_tagged_fields();
    }
}

std::vector<std::uint8_t> make_response_frame(
    const RequestHeader& request,
    const std::vector<std::uint8_t>& body) {
    WireWriter writer;
    writer.write_int32(0);
    write_response_header(writer, request);
    writer.write_raw(body);
    writer.patch_int32(0, static_cast<std::int32_t>(writer.size() - 4));
    return writer.take_bytes();
}

}  // namespace mkmq::protocol
