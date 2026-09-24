#ifndef STREAMFORGE_MESSAGE_HANDLER_HPP
#define STREAMFORGE_MESSAGE_HANDLER_HPP

#include "streamforge/FrameCodec.hpp"
#include "streamforge/TopicManager.hpp"

namespace streamforge {

class MessageHandler {
public:
    explicit MessageHandler(TopicManager& topic_mgr);

    Frame handle_request(const Frame& request);

private:
    TopicManager& m_topic_mgr;

    Frame handle_create_topic(const Frame& req);
    Frame handle_produce(const Frame& req);
    Frame handle_fetch(const Frame& req);
    Frame handle_list_topics(const Frame& req);
    Frame handle_describe_topic(const Frame& req);

    static uint16_t map_status_to_error_code(const Status& status);
};

} // namespace streamforge

#endif // STREAMFORGE_MESSAGE_HANDLER_HPP
