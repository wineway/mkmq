#pragma once

#include "mkmq/protocol/api.hpp"
#include "mkmq/protocol/wire.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace mkmq::protocol {

struct RequestHeader {
    ApiKey api_key{ApiKey::ApiVersions};
    std::int16_t api_version{0};
    std::int32_t correlation_id{0};
    std::string client_id;
    std::int16_t header_version{1};
};

RequestHeader parse_request_header(WireReader& reader);
void write_response_header(WireWriter& writer, const RequestHeader& request);

std::vector<std::uint8_t> make_response_frame(
    const RequestHeader& request,
    const std::vector<std::uint8_t>& body);

}  // namespace mkmq::protocol
