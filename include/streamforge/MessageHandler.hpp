#ifndef STREAMFORGE_MESSAGE_HANDLER_HPP
#define STREAMFORGE_MESSAGE_HANDLER_HPP

#include "streamforge/FrameCodec.hpp"
#include "streamforge/TopicManager.hpp"
#include "streamforge/GroupCoordinator.hpp"

namespace streamforge {

class MessageHandler {
public:
    explicit MessageHandler(TopicManager& topic_mgr, GroupCoordinator* coordinator = nullptr);

    Frame handle_request(const Frame& request);

private:
    TopicManager& m_topic_mgr;
    GroupCoordinator* m_coordinator;

    Frame handle_create_topic(const Frame& req);
    Frame handle_produce(const Frame& req);
    Frame handle_fetch(const Frame& req);
    Frame handle_list_topics(const Frame& req);
    Frame handle_describe_topic(const Frame& req);

    Frame handle_join_group(const Frame& req);
    Frame handle_heartbeat(const Frame& req);
    Frame handle_leave_group(const Frame& req);
    Frame handle_commit_offset(const Frame& req);
    Frame handle_fetch_offset(const Frame& req);
    Frame handle_describe_group(const Frame& req);

    static uint16_t map_status_to_error_code(const Status& status);
};

} // namespace streamforge

#endif // STREAMFORGE_MESSAGE_HANDLER_HPP
