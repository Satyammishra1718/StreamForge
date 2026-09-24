#include "streamforge/MessageHandler.hpp"
#include "streamforge/ProtocolMessages.hpp"
#include "streamforge/Logger.hpp"
#include <algorithm>

namespace streamforge {

MessageHandler::MessageHandler(TopicManager& topic_mgr)
    : m_topic_mgr(topic_mgr) {}

uint16_t MessageHandler::map_status_to_error_code(const Status& status) {
    switch (status.code()) {
        case StatusCode::NotFound:          return ErrorCode::UNKNOWN_TOPIC;
        case StatusCode::AlreadyExists:     return ErrorCode::TOPIC_ALREADY_EXISTS;
        case StatusCode::InvalidArgument:   return ErrorCode::INVALID_ARGUMENT;
        case StatusCode::Corrupt:           return ErrorCode::CORRUPT_DATA;
        case StatusCode::OffsetOutOfRange:  return ErrorCode::OFFSET_OUT_OF_RANGE;
        case StatusCode::RecordTooLarge:    return ErrorCode::RECORD_TOO_LARGE;
        case StatusCode::IoError:           return ErrorCode::INTERNAL_ERROR;
        default:                            return ErrorCode::INTERNAL_ERROR;
    }
}

Frame MessageHandler::handle_request(const Frame& request) {
    switch (request.type) {
        case MessageType::PING:
            Logger::instance().debug("Handled PING (req_id=" + std::to_string(request.request_id) + ")");
            return FrameCodec::create_pong_frame(request.request_id);

        case MessageType::ECHO:
            Logger::instance().debug("Handled ECHO (req_id=" + std::to_string(request.request_id) + ")");
            return FrameCodec::create_echo_reply_frame(request.request_id, request.body);

        case MessageType::CREATE_TOPIC:
            return handle_create_topic(request);

        case MessageType::PRODUCE:
            return handle_produce(request);

        case MessageType::FETCH:
            return handle_fetch(request);

        case MessageType::LIST_TOPICS:
            return handle_list_topics(request);

        case MessageType::DESCRIBE_TOPIC:
            return handle_describe_topic(request);

        default:
            Logger::instance().warning("Unknown message type: 0x" + std::to_string(static_cast<int>(request.type)));
            return FrameCodec::create_error_frame(
                request.request_id,
                ErrorCode::UNKNOWN_TYPE,
                "Unknown message type: 0x" + std::to_string(static_cast<int>(request.type))
            );
    }
}

Frame MessageHandler::handle_create_topic(const Frame& req) {
    BodyReader reader(req.body);
    CreateTopicRequest req_msg;
    if (!req_msg.decode(reader)) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::MALFORMED_BODY, "Malformed CREATE_TOPIC body");
    }

    if (req_msg.partitions == 0) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::INVALID_ARGUMENT, "Partitions count must be > 0");
    }

    std::string name_err;
    if (!TopicManager::validate_topic_name(req_msg.topic, name_err)) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::INVALID_TOPIC_NAME, "Invalid topic name: " + name_err);
    }

    auto res = m_topic_mgr.create_topic(req_msg.topic, req_msg.partitions);
    if (!res.ok()) {
        return FrameCodec::create_error_frame(req.request_id, map_status_to_error_code(res.status()), res.status().message());
    }

    Logger::instance().debug("Created topic '" + req_msg.topic + "' with " + std::to_string(req_msg.partitions) + " partitions");
    return Frame{ HEADER_SIZE, MessageType::CREATE_TOPIC_OK, req.request_id, {} };
}

Frame MessageHandler::handle_produce(const Frame& req) {
    BodyReader reader(req.body);
    ProduceRequest req_msg;
    if (!req_msg.decode(reader)) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::MALFORMED_BODY, "Malformed PRODUCE body");
    }

    if (req_msg.records.empty()) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::INVALID_ARGUMENT, "PRODUCE batch cannot be empty");
    }

    auto topic = m_topic_mgr.get_topic(req_msg.topic);
    if (!topic) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::UNKNOWN_TOPIC, "Topic '" + req_msg.topic + "' not found");
    }

    uint32_t chosen_partition = 0;
    auto res = topic->produce_batch(req_msg.partition, req_msg.records, chosen_partition);
    if (!res.ok()) {
        return FrameCodec::create_error_frame(req.request_id, map_status_to_error_code(res.status()), res.status().message());
    }

    ProduceResponse resp;
    resp.partition = static_cast<uint16_t>(chosen_partition);
    resp.base_offset = res.value();
    resp.count = static_cast<uint16_t>(req_msg.records.size());

    BodyWriter writer;
    resp.encode(writer);
    std::vector<uint8_t> body = writer.take_buffer();

    Logger::instance().debug("Produced " + std::to_string(resp.count) + " records to topic '" + req_msg.topic +
                             "' P" + std::to_string(resp.partition) + " base_offset=" + std::to_string(resp.base_offset));

    return Frame{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::PRODUCE_OK, req.request_id, body };
}

Frame MessageHandler::handle_fetch(const Frame& req) {
    BodyReader reader(req.body);
    FetchRequest req_msg;
    if (!req_msg.decode(reader)) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::MALFORMED_BODY, "Malformed FETCH body");
    }

    if (req_msg.max_bytes == 0 || req_msg.max_messages == 0) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::INVALID_ARGUMENT, "max_bytes and max_messages must be > 0");
    }

    auto topic = m_topic_mgr.get_topic(req_msg.topic);
    if (!topic) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::UNKNOWN_TOPIC, "Topic '" + req_msg.topic + "' not found");
    }

    if (req_msg.partition >= topic->num_partitions()) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::INVALID_PARTITION, "Partition " + std::to_string(req_msg.partition) + " out of range");
    }

    auto part = topic->get_partition(req_msg.partition);
    uint64_t hw = part->next_offset();
    uint64_t earliest = part->earliest_offset();

    FetchResponse resp;
    resp.high_watermark = hw;
    resp.earliest_offset = earliest;

    if (req_msg.start_offset == hw) {
        // Requirement 2: FETCH at start_offset == high_watermark returns FETCH_OK with zero records (caught up), NOT an error.
        resp.next_offset = hw;
        resp.record_count = 0;
    } else if (req_msg.start_offset > hw || req_msg.start_offset < earliest) {
        return FrameCodec::create_error_frame(
            req.request_id,
            ErrorCode::OFFSET_OUT_OF_RANGE,
            "Start offset " + std::to_string(req_msg.start_offset) + " out of valid range [" +
            std::to_string(earliest) + ", " + std::to_string(hw) + ")"
        );
    } else {
        // Requirement 2: Clamp max_bytes so response frame can never exceed 1 MiB limit (leaving room for header).
        uint32_t clamped_max_bytes = std::min(req_msg.max_bytes, static_cast<uint32_t>(1048500));
        ReadResult rr = part->read(req_msg.start_offset, req_msg.max_messages, clamped_max_bytes);
        if (!rr.status.ok()) {
            return FrameCodec::create_error_frame(req.request_id, map_status_to_error_code(rr.status), rr.status.message());
        }

        for (const auto& r : rr.records) {
            FetchRecordWire fr;
            fr.offset = r.offset;
            fr.timestamp_ms = r.timestamp_ms;
            fr.key = r.key;
            fr.value = r.value;
            resp.records.push_back(std::move(fr));
        }

        resp.record_count = static_cast<uint16_t>(resp.records.size());
        if (resp.records.empty()) {
            resp.next_offset = req_msg.start_offset;
        } else {
            resp.next_offset = resp.records.back().offset + 1;
        }
    }

    BodyWriter writer;
    resp.encode(writer);
    std::vector<uint8_t> body = writer.take_buffer();

    Logger::instance().debug("Fetched " + std::to_string(resp.record_count) + " records from topic '" + req_msg.topic +
                             "' P" + std::to_string(req_msg.partition) + " start_offset=" + std::to_string(req_msg.start_offset) +
                             " next_offset=" + std::to_string(resp.next_offset));

    return Frame{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::FETCH_OK, req.request_id, body };
}

Frame MessageHandler::handle_list_topics(const Frame& req) {
    BodyReader reader(req.body);
    ListTopicsRequest req_msg;
    if (!req_msg.decode(reader)) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::MALFORMED_BODY, "Malformed LIST_TOPICS body");
    }

    auto topic_list = m_topic_mgr.list_topics();
    ListTopicsResponse resp;
    for (const auto& t : topic_list) {
        resp.topics.push_back({ t->name(), static_cast<uint16_t>(t->num_partitions()) });
    }
    resp.count = static_cast<uint16_t>(resp.topics.size());

    BodyWriter writer;
    resp.encode(writer);
    std::vector<uint8_t> body = writer.take_buffer();

    Logger::instance().debug("Listed " + std::to_string(resp.count) + " topics");
    return Frame{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::TOPICS, req.request_id, body };
}

Frame MessageHandler::handle_describe_topic(const Frame& req) {
    BodyReader reader(req.body);
    DescribeTopicRequest req_msg;
    if (!req_msg.decode(reader)) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::MALFORMED_BODY, "Malformed DESCRIBE_TOPIC body");
    }

    auto topic = m_topic_mgr.get_topic(req_msg.topic);
    if (!topic) {
        return FrameCodec::create_error_frame(req.request_id, ErrorCode::UNKNOWN_TOPIC, "Topic '" + req_msg.topic + "' not found");
    }

    DescribeTopicResponse resp;
    resp.name = topic->name();
    resp.partitions = static_cast<uint16_t>(topic->num_partitions());
    for (uint32_t p = 0; p < topic->num_partitions(); ++p) {
        auto part = topic->get_partition(p);
        resp.partition_offsets.push_back({ part->earliest_offset(), part->next_offset() });
    }

    BodyWriter writer;
    resp.encode(writer);
    std::vector<uint8_t> body = writer.take_buffer();

    Logger::instance().debug("Described topic '" + req_msg.topic + "' (" + std::to_string(resp.partitions) + " partitions)");
    return Frame{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::TOPIC_DESCRIPTION, req.request_id, body };
}

} // namespace streamforge
