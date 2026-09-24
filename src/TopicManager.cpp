#include "streamforge/TopicManager.hpp"
#include "streamforge/Logger.hpp"
#include "streamforge/FileHandle.hpp"
#include <fstream>
#include <algorithm>
#include <cctype>
#include <chrono>

namespace streamforge {

TopicManager::TopicManager(StorageConfig config)
    : m_config(std::move(config)) {}

bool TopicManager::validate_topic_name(const std::string& name, std::string& out_error) {
    if (name.empty()) {
        out_error = "Topic name cannot be empty";
        return false;
    }
    if (name.length() > 249) {
        out_error = "Topic name exceeds maximum length of 249 characters";
        return false;
    }

    for (char c : name) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '.' && c != '_' && c != '-') {
            out_error = "Topic name contains invalid character: '" + std::string(1, c) + "'";
            return false;
        }
    }

    if (name == "." || name == "..") {
        out_error = "Topic name cannot be '.' or '..'";
        return false;
    }

    // Windows reserved device names
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
    ofs << "retention_ms=" << topic.retention_ms() << "\n";
    ofs << "retention_bytes=" << topic.retention_bytes() << "\n";
    ofs.flush();
    FileHandle::flush_directory(topic.topic_dir());
    return Status::OK();
}

Status TopicManager::load_topic_metadata(const std::filesystem::path& topic_dir, uint32_t& out_partitions,
                                         uint64_t& out_retention_ms, uint64_t& out_retention_bytes) {
    std::filesystem::path meta_path = topic_dir / "topic.meta";
    std::ifstream ifs(meta_path);
    if (!ifs.is_open()) {
        return Status::NotFound("Topic metadata file missing: " + meta_path.string());
    }

    out_partitions = 0;
    out_retention_ms = m_config.default_retention_ms;
    out_retention_bytes = m_config.default_retention_bytes;
    std::string line;
    while (std::getline(ifs, line)) {
        auto pos = line.find('=');
        if (pos != std::string::npos) {
            std::string key = line.substr(0, pos);
            std::string val = line.substr(pos + 1);
            if (key == "partitions") {
                try {
                    out_partitions = static_cast<uint32_t>(std::stoul(val));
                } catch (...) {}
            } else if (key == "retention_ms") {
                try {
                    out_retention_ms = std::stoull(val);
                } catch (...) {}
            } else if (key == "retention_bytes") {
                try {
                    out_retention_bytes = std::stoull(val);
                } catch (...) {}
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
        FileHandle::flush_directory(base_path);
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
            uint64_t retention_ms = m_config.default_retention_ms;
            uint64_t retention_bytes = m_config.default_retention_bytes;
            Status st = load_topic_metadata(entry.path(), partitions, retention_ms, retention_bytes);
            if (!st.ok()) {
                Logger::instance().warning("Failed to load metadata for topic " + topic_name + ": " + st.message());
                continue;
            }

            auto topic = std::make_shared<Topic>(topic_name, partitions, entry.path(), m_config, retention_ms, retention_bytes);
            st = topic->open_and_recover();
            if (!st.ok()) {
                Logger::instance().error("Failed to recover topic " + topic_name + ": " + st.message());
                return st;
            }

            m_topics[topic_name] = topic;
            Logger::instance().info("Loaded topic '" + topic_name + "' (" + std::to_string(partitions) + " partitions, retention_ms=" +
                                   std::to_string(retention_ms) + ", retention_bytes=" + std::to_string(retention_bytes) + ")");
        }
    }

    return Status::OK();
}

Result<std::shared_ptr<Topic>> TopicManager::create_topic(const std::string& name, uint32_t partitions,
                                                          uint64_t retention_ms, uint64_t retention_bytes) {
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

    if (retention_ms == 0) {
        retention_ms = m_config.default_retention_ms;
    }
    if (retention_bytes == 0 && m_config.default_retention_bytes > 0) {
        retention_bytes = m_config.default_retention_bytes;
    }

    std::filesystem::path topic_dir = std::filesystem::path(m_config.data_dir) / name;
    if (std::filesystem::exists(topic_dir / "topic.meta")) {
        return Status::AlreadyExists("Topic metadata already exists on disk for '" + name + "'");
    }

    auto topic = std::make_shared<Topic>(name, partitions, topic_dir, m_config, retention_ms, retention_bytes);
    Status st = topic->open_and_recover();
    if (!st.ok()) {
        return st;
    }

    st = save_topic_metadata(*topic);
    if (!st.ok()) {
        return st;
    }

    FileHandle::flush_directory(topic_dir);
    FileHandle::flush_directory(std::filesystem::path(m_config.data_dir));

    m_topics[name] = topic;
    Logger::instance().info("Created topic '" + name + "' with " + std::to_string(partitions) + " partitions (retention_ms=" +
                           std::to_string(retention_ms) + ", retention_bytes=" + std::to_string(retention_bytes) + ")");
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
