#include "streamforge/TopicManager.hpp"
#include "streamforge/Logger.hpp"
#include <fstream>
#include <sstream>
#include <cctype>
#include <algorithm>
#include <chrono>

namespace streamforge {

TopicManager::TopicManager(StorageConfig config)
    : m_config(std::move(config)) {}

bool TopicManager::validate_topic_name(const std::string& name, std::string& out_error) {
    if (name.empty() || name.size() > 64) {
        out_error = "Topic name must be between 1 and 64 characters";
        return false;
    }
    if (name == "." || name == "..") {
        out_error = "Topic name cannot be '.' or '..'";
        return false;
    }
    if (name.front() == '.' || name.back() == '.') {
        out_error = "Topic name cannot start or end with '.'";
        return false;
    }
    if (name.find("..") != std::string::npos) {
        out_error = "Topic name cannot contain '..'";
        return false;
    }

    for (char c : name) {
        if (!((c >= 'A' && c <= 'Z') ||
              (c >= 'a' && c <= 'z') ||
              (c >= '0' && c <= '9') ||
              c == '.' || c == '_' || c == '-')) {
            out_error = "Topic name contains invalid character: '" + std::string(1, c) + "'";
            return false;
        }
    }

    static const char* reserved_names[] = {
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
    };

    std::string upper_name = name;
    for (char& c : upper_name) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }

    for (const char* res : reserved_names) {
        if (upper_name == res) {
            out_error = "Topic name '" + name + "' is a Windows reserved device name";
            return false;
        }
    }

    return true;
}

Status TopicManager::save_topic_metadata(const Topic& topic) {
    std::filesystem::path meta_path = topic.topic_dir() / "topic.meta";
    std::ofstream ofs(meta_path);
    if (!ofs.is_open()) {
        return Status::IoError("Failed to write topic metadata file: " + meta_path.string());
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    ofs << "partitions=" << topic.num_partitions() << "\n";
    ofs << "created_ms=" << now_ms << "\n";
    return Status::OK();
}

Status TopicManager::load_topic_metadata(const std::filesystem::path& topic_dir, uint32_t& out_partitions) {
    std::filesystem::path meta_path = topic_dir / "topic.meta";
    std::ifstream ifs(meta_path);
    if (!ifs.is_open()) {
        return Status::NotFound("Topic metadata file missing: " + meta_path.string());
    }

    out_partitions = 0;
    std::string line;
    while (std::getline(ifs, line)) {
        auto pos = line.find('=');
        if (pos != std::string::npos) {
            std::string key = line.substr(0, pos);
            std::string val = line.substr(pos + 1);
            if (key == "partitions") {
                out_partitions = static_cast<uint32_t>(std::stoul(val));
            }
        }
    }

    if (out_partitions == 0) {
        return Status::Corrupt("Invalid partitions count in metadata file: " + meta_path.string());
    }

    return Status::OK();
}

Status TopicManager::open_and_recover_all() {
    std::unique_lock<std::shared_mutex> lock(m_mutex);
    m_topics.clear();

    std::filesystem::path base_path(m_config.data_dir);
    if (!std::filesystem::exists(base_path)) {
        std::filesystem::create_directories(base_path);
        return Status::OK();
    }

    for (const auto& entry : std::filesystem::directory_iterator(base_path)) {
        if (entry.is_directory()) {
            std::string topic_name = entry.path().filename().string();
            std::string err;
            if (!validate_topic_name(topic_name, err)) {
                Logger::instance().warning("Skipping invalid topic directory: " + topic_name + " (" + err + ")");
                continue;
            }

            uint32_t partitions = 0;
            Status st = load_topic_metadata(entry.path(), partitions);
            if (!st.ok()) {
                Logger::instance().warning("Failed to load metadata for topic " + topic_name + ": " + st.message());
                continue;
            }

            auto topic = std::make_shared<Topic>(topic_name, partitions, entry.path(), m_config);
            st = topic->open_and_recover();
            if (!st.ok()) {
                Logger::instance().error("Failed to recover topic " + topic_name + ": " + st.message());
                return st;
            }

            m_topics[topic_name] = topic;
            Logger::instance().info("Loaded topic '" + topic_name + "' (" + std::to_string(partitions) + " partitions)");
        }
    }

    return Status::OK();
}

Result<std::shared_ptr<Topic>> TopicManager::create_topic(const std::string& name, uint32_t partitions) {
    std::unique_lock<std::shared_mutex> lock(m_mutex);

    std::string err;
    if (!validate_topic_name(name, err)) {
        return Status::InvalidArgument(err);
    }

    if (partitions < 1 || partitions > 64) {
        return Status::InvalidArgument("Partitions count must be between 1 and 64");
    }

    if (m_topics.find(name) != m_topics.end()) {
        return Status::AlreadyExists("Topic '" + name + "' already exists");
    }

    std::filesystem::path topic_dir = std::filesystem::path(m_config.data_dir) / name;
    if (std::filesystem::exists(topic_dir / "topic.meta")) {
        return Status::AlreadyExists("Topic metadata already exists on disk for '" + name + "'");
    }

    auto topic = std::make_shared<Topic>(name, partitions, topic_dir, m_config);
    Status st = topic->open_and_recover();
    if (!st.ok()) {
        return st;
    }

    st = save_topic_metadata(*topic);
    if (!st.ok()) {
        return st;
    }

    m_topics[name] = topic;
    Logger::instance().info("Created topic '" + name + "' with " + std::to_string(partitions) + " partitions");
    return topic;
}

std::shared_ptr<Topic> TopicManager::get_topic(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(m_mutex);
    auto it = m_topics.find(name);
    if (it == m_topics.end()) {
        return nullptr;
    }
    return it->second;
}

std::vector<std::shared_ptr<Topic>> TopicManager::list_topics() const {
    std::shared_lock<std::shared_mutex> lock(m_mutex);
    std::vector<std::shared_ptr<Topic>> list;
    list.reserve(m_topics.size());
    for (const auto& pair : m_topics) {
        list.push_back(pair.second);
    }
    return list;
}

} // namespace streamforge
