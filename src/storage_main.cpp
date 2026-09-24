#include "streamforge/TopicManager.hpp"
#include "streamforge/RecordCodec.hpp"
#include "streamforge/FileHandle.hpp"
#include "streamforge/RetentionManager.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <iomanip>

using namespace streamforge;

static void print_usage() {
    std::cout << "Usage:\n"
              << "  streamforge_storage --data-dir DIR create-topic NAME PARTITIONS [--retention-ms N] [--retention-bytes N]\n"
              << "  streamforge_storage --data-dir DIR append TOPIC VALUE [--key K] [--partition P]\n"
              << "  streamforge_storage --data-dir DIR read TOPIC PARTITION START_OFFSET [--max N]\n"
              << "  streamforge_storage --data-dir DIR describe TOPIC\n"
              << "  streamforge_storage --data-dir DIR fill TOPIC COUNT VALUE_SIZE [--key-prefix P] [--segment-bytes N]\n"
              << "  streamforge_storage --data-dir DIR gc TOPIC [--dry-run]\n"
              << "  streamforge_storage dump-segment PATH_TO_LOG\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::string data_dir = "./data";
    uint32_t segment_bytes = DEFAULT_SEGMENT_MAX_BYTES;
    std::string command;
    std::vector<std::string> args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--segment-bytes" && i + 1 < argc) {
            segment_bytes = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (command.empty() && arg.rfind("--", 0) != 0) {
            command = arg;
        } else {
            args.push_back(arg);
        }
    }

    if (command == "dump-segment") {
        if (args.empty()) {
            std::cerr << "Error: dump-segment requires PATH_TO_LOG argument\n";
            return 1;
        }
        std::filesystem::path log_path = args[0];
        FileHandle file = FileHandle::open_read_only(log_path);
        if (!file.is_valid()) {
            std::cerr << "Error: Failed to open log file: " << log_path.string() << "\n";
            return 1;
        }

        uint64_t file_size = file.get_size();
        uint64_t pos = 0;
        std::cout << "Dumping log segment: " << log_path.string() << " (" << file_size << " bytes)\n";

        size_t count = 0;
        while (pos < file_size) {
            uint8_t header[28];
            if (!file.read_at(pos, header, 28)) {
                std::cout << "Position " << pos << ": [TRUNCATED HEADER]\n";
                break;
            }

            uint32_t length = RecordCodec::read_u32(header);
            if (length < 24 || length > MAX_RECORD_SIZE) {
                std::cout << "Position " << pos << ": [INVALID LENGTH " << length << "]\n";
                break;
            }

            uint32_t total_size = 4 + length;
            if (pos + total_size > file_size) {
                std::cout << "Position " << pos << ": [PARTIAL RECORD Payload " << (file_size - pos) << "/" << total_size << " bytes]\n";
                break;
            }

            std::vector<uint8_t> buf(total_size);
            file.read_at(pos, buf.data(), total_size);

            Record record;
            Status st = RecordCodec::decode(buf.data(), buf.size(), record);

            std::cout << "[" << std::setw(5) << count << "] Offset: " << std::setw(8) << record.offset
                      << " Pos: " << std::setw(8) << pos
                      << " Size: " << std::setw(6) << total_size
                      << " CRC: " << (st.ok() ? "VALID" : ("CORRUPT (" + st.message() + ")"))
                      << " Key: " << std::string(record.key.begin(), record.key.end())
                      << " Val: " << std::string(record.value.begin(), record.value.end()) << "\n";

            pos += total_size;
            count++;
        }
        return 0;
    }

    StorageConfig config;
    config.data_dir = data_dir;
    config.segment_max_bytes = segment_bytes;

    TopicManager manager(config);
    Status st = manager.open_and_recover_all();
    if (!st.ok()) {
        std::cerr << "Error initializing storage manager: " << st.message() << "\n";
        return 1;
    }

    if (command == "create-topic") {
        if (args.size() < 2) {
            std::cerr << "Usage: create-topic NAME PARTITIONS [--retention-ms N] [--retention-bytes N]\n";
            return 1;
        }
        std::string name = args[0];
        uint32_t partitions = static_cast<uint32_t>(std::stoul(args[1]));
        uint64_t ret_ms = 0;
        uint64_t ret_bytes = 0;
        for (size_t i = 2; i < args.size(); ++i) {
            if (args[i] == "--retention-ms" && i + 1 < args.size()) {
                ret_ms = std::stoull(args[++i]);
            } else if (args[i] == "--retention-bytes" && i + 1 < args.size()) {
                ret_bytes = std::stoull(args[++i]);
            }
        }

        auto res = manager.create_topic(name, partitions, ret_ms, ret_bytes);
        if (!res.ok()) {
            std::cerr << "Failed to create topic: " << res.status().message() << "\n";
            return 1;
        }
        std::cout << "Successfully created topic '" << name << "' with " << partitions << " partitions.\n";
        return 0;

    } else if (command == "append") {
        if (args.empty()) {
            std::cerr << "Usage: append TOPIC VALUE [--key K] [--partition P]\n";
            return 1;
        }
        std::string topic_name = args[0];
        std::string value_str;
        std::string key_str;
        int target_partition = -1;

        for (size_t i = 1; i < args.size(); ++i) {
            if (args[i] == "--key" && i + 1 < args.size()) {
                key_str = args[++i];
            } else if (args[i] == "--partition" && i + 1 < args.size()) {
                target_partition = std::stoi(args[++i]);
            } else if (value_str.empty()) {
                value_str = args[i];
            }
        }

        auto topic = manager.get_topic(topic_name);
        if (!topic) {
            std::cerr << "Error: Topic '" << topic_name << "' not found.\n";
            return 1;
        }

        std::vector<uint8_t> key(key_str.begin(), key_str.end());
        std::vector<uint8_t> val(value_str.begin(), value_str.end());

        Result<uint64_t> res = (target_partition >= 0)
            ? topic->append_to(static_cast<uint32_t>(target_partition), key, val)
            : topic->append(key, val);

        if (!res.ok()) {
            std::cerr << "Append failed: " << res.status().message() << "\n";
            return 1;
        }

        std::cout << "Appended to topic '" << topic_name << "' -> Partition: " << target_partition << ", Offset: " << res.value() << "\n";
        return 0;

    } else if (command == "read") {
        if (args.size() < 3) {
            std::cerr << "Usage: read TOPIC PARTITION START_OFFSET [--max N]\n";
            return 1;
        }
        std::string topic_name = args[0];
        uint32_t part_id = static_cast<uint32_t>(std::stoul(args[1]));
        uint64_t start_off = std::stoull(args[2]);
        size_t max_msgs = 100;

        for (size_t i = 3; i < args.size(); ++i) {
            if (args[i] == "--max" && i + 1 < args.size()) {
                max_msgs = std::stoul(args[++i]);
            }
        }

        auto topic = manager.get_topic(topic_name);
        if (!topic) {
            std::cerr << "Error: Topic '" << topic_name << "' not found.\n";
            return 1;
        }

        auto partition = topic->get_partition(part_id);
        if (!partition) {
            std::cerr << "Error: Partition " << part_id << " not found.\n";
            return 1;
        }

        ReadResult rr = partition->read(start_off, max_msgs);
        if (!rr.status.ok()) {
            std::cerr << "Read error: " << rr.status.message() << "\n";
            return 1;
        }

        std::cout << "Read " << rr.records.size() << " records from " << topic_name << "[" << part_id << "]:\n";
        for (const auto& rec : rr.records) {
            std::string k(rec.key.begin(), rec.key.end());
            std::string v(rec.value.begin(), rec.value.end());
            std::cout << "Offset: " << rec.offset << " | Timestamp: " << rec.timestamp_ms
                      << " | Key: " << k << " | Value: " << v << "\n";
        }
        return 0;

    } else if (command == "describe") {
        if (args.empty()) {
            std::cerr << "Usage: describe TOPIC\n";
            return 1;
        }
        std::string topic_name = args[0];
        auto topic = manager.get_topic(topic_name);
        if (!topic) {
            std::cerr << "Error: Topic '" << topic_name << "' not found.\n";
            return 1;
        }

        std::cout << "Topic: " << topic_name << " (Partitions: " << topic->num_partitions() << ")\n";
        for (uint32_t p = 0; p < topic->num_partitions(); ++p) {
            auto part = topic->get_partition(p);
            std::cout << "  Partition " << p << ": EarliestOffset=" << part->earliest_offset()
                      << ", NextOffset=" << part->next_offset()
                      << ", Segments=" << part->segment_count()
                      << ", TotalBytes=" << part->total_bytes() << "\n";
        }
        return 0;

    } else if (command == "fill") {
        if (args.size() < 3) {
            std::cerr << "Usage: fill TOPIC COUNT VALUE_SIZE [--key-prefix P] [--segment-bytes N]\n";
            return 1;
        }
        std::string topic_name = args[0];
        size_t count = std::stoul(args[1]);
        size_t val_size = std::stoul(args[2]);
        std::string key_prefix = "key-";

        for (size_t i = 3; i < args.size(); ++i) {
            if (args[i] == "--key-prefix" && i + 1 < args.size()) {
                key_prefix = args[++i];
            }
        }

        auto topic = manager.get_topic(topic_name);
        if (!topic) {
            std::cerr << "Error: Topic '" << topic_name << "' not found.\n";
            return 1;
        }

        std::vector<uint8_t> dummy_val(val_size, 'X');
        std::cout << "Filling topic '" << topic_name << "' with " << count << " records of " << val_size << " bytes each...\n";

        for (size_t i = 0; i < count; ++i) {
            std::string k = key_prefix + std::to_string(i);
            std::vector<uint8_t> key(k.begin(), k.end());
            auto res = topic->append(key, dummy_val);
            if (!res.ok()) {
                std::cerr << "Fill stopped on error at index " << i << ": " << res.status().message() << "\n";
                return 1;
            }
        }
        std::cout << "Successfully filled " << count << " records into '" << topic_name << "'.\n";
        return 0;

    } else if (command == "gc") {
        if (args.empty()) {
            std::cerr << "Usage: streamforge_storage --data-dir DIR gc TOPIC [--dry-run]\n";
            return 1;
        }
        std::string topic_name = args[0];
        bool dry_run = false;
        for (size_t i = 1; i < args.size(); ++i) {
            if (args[i] == "--dry-run") {
                dry_run = true;
            }
        }

        auto topic = manager.get_topic(topic_name);
        if (!topic) {
            std::cerr << "Error: Topic '" << topic_name << "' not found.\n";
            return 1;
        }

        RetentionManager retention_mgr(manager);
        size_t count = retention_mgr.run_one_pass(dry_run, topic_name);
        if (dry_run) {
            std::cout << "GC dry-run completed: " << count << " segment(s) would be deleted for topic '" << topic_name << "'.\n";
        } else {
            std::cout << "GC completed: " << count << " segment(s) deleted for topic '" << topic_name << "'.\n";
        }
        return 0;

    } else {
        std::cerr << "Unknown command: " << command << "\n";
        print_usage();
        return 1;
    }
}
