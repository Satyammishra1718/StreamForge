#ifndef STREAMFORGE_MESSAGE_HANDLER_HPP
#define STREAMFORGE_MESSAGE_HANDLER_HPP

#include "streamforge/FrameCodec.hpp"

namespace streamforge {

class MessageHandler {
public:
    MessageHandler() = default;

    Frame handle_request(const Frame& request);
};

} // namespace streamforge

#endif // STREAMFORGE_MESSAGE_HANDLER_HPP
