#ifndef STREAMFORGE_TEST_FRAMEWORK_HPP
#define STREAMFORGE_TEST_FRAMEWORK_HPP

#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <sstream>
#include <cstdlib>

namespace streamforge::test {

struct TestInfo {
    std::string name;
    std::function<void()> func;
};

class TestRegistry {
public:
    static TestRegistry& instance() {
        static TestRegistry reg;
        return reg;
    }

    void add_test(const std::string& name, std::function<void()> func) {
        m_tests.push_back({name, func});
    }

    int run_all() {
        int passed = 0;
        int failed = 0;
        std::cout << "========== Running StreamForge Storage Unit Tests ==========" << std::endl;
        for (const auto& test : m_tests) {
            std::cout << "[ RUN      ] " << test.name << std::endl;
            m_current_failed = false;
            m_current_test_name = test.name;

            try {
                test.func();
            } catch (const std::exception& ex) {
                std::cout << "  [ EXCEPTION ] " << ex.what() << std::endl;
                m_current_failed = true;
            } catch (...) {
                std::cout << "  [ EXCEPTION ] Unknown exception thrown" << std::endl;
                m_current_failed = true;
            }

            if (!m_current_failed) {
                std::cout << "[       OK ] " << test.name << std::endl;
                passed++;
            } else {
                std::cout << "[  FAILED  ] " << test.name << std::endl;
                failed++;
            }
        }

        std::cout << "============================================================" << std::endl;
        std::cout << "Test Summary: " << passed << " passed, " << failed << " failed." << std::endl;
        std::cout << "============================================================" << std::endl;
        return (failed == 0) ? 0 : 1;
    }

    void report_failure(const std::string& file, int line, const std::string& expr, const std::string& msg = "") {
        m_current_failed = true;
        std::cout << "  " << file << ":" << line << ": Failure: (" << expr << ") " << msg << std::endl;
    }

private:
    std::vector<TestInfo> m_tests;
    bool m_current_failed{false};
    std::string m_current_test_name;
};

struct RegisterHelper {
    RegisterHelper(const std::string& name, std::function<void()> func) {
        TestRegistry::instance().add_test(name, func);
    }
};

} // namespace streamforge::test

#define TEST_CASE(name) \
    static void test_func_##name(); \
    static ::streamforge::test::RegisterHelper reg_##name(#name, test_func_##name); \
    static void test_func_##name()

#define CHECK(expr) \
    do { \
        if (!(expr)) { \
            ::streamforge::test::TestRegistry::instance().report_failure(__FILE__, __LINE__, #expr); \
        } \
    } while(0)

#define CHECK_TRUE(expr) CHECK(expr)
#define CHECK_FALSE(expr) CHECK(!(expr))

#define CHECK_EQ(a, b) \
    do { \
        auto val_a = (a); \
        auto val_b = (b); \
        if (!(val_a == val_b)) { \
            std::ostringstream ss; \
            ss << "Expected " << val_b << ", got " << val_a; \
            ::streamforge::test::TestRegistry::instance().report_failure(__FILE__, __LINE__, #a " == " #b, ss.str()); \
        } \
    } while(0)

#endif // STREAMFORGE_TEST_FRAMEWORK_HPP
