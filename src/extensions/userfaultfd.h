#pragma once

#include <linux/userfaultfd.h>
#include <sys/ioctl.h>
#include <fcntl.h>

#ifndef UFFDIO_CONTINUE_MODE_WP
#define UFFDIO_CONTINUE_MODE_WP ((__u64)1 << 1)
#endif

#ifndef UFFD_FEATURE_WP_HUGETLBFS_SHMEM
#define UFFD_FEATURE_WP_HUGETLBFS_SHMEM (1<<12)
#endif


class UserFaultFD {
public:
    UserFaultFD() {
        fd = syscall(SYS_userfaultfd, O_NONBLOCK | UFFD_USER_MODE_ONLY);
        if (fd < 0) throw std::runtime_error("Error: userfaultfd syscall failed, make sure you have root privileges | Line: " + std::to_string(__LINE__));

        uffdio_api api{ .api = UFFD_API };
        if (ioctl(fd, UFFDIO_API, &api) == -1)
            throw std::runtime_error("Error: UFFDIO_API ioctl failed | Line: " + std::to_string(__LINE__));

        auto printAndThrow = [&](const char* featureName) {
            printf("Your kernel doesn't support %s, which is required for write-protect page fault handling. Please upgrade to Linux kernel 6.5 or later.\n", featureName);
            printf("Run: sudo apt update && sudo apt install linux-generic-hwe-22.04 -y to get a newer kernel on Ubuntu\n");
            throw std::runtime_error(std::string("Error: ") + featureName + " not supported | Line: " + std::to_string(__LINE__));
        };

        if (!(api.features & UFFD_FEATURE_WP_HUGETLBFS_SHMEM)) {
            printAndThrow("UFFD_FEATURE_WP_HUGETLBFS_SHMEM");
        }
    }

    ~UserFaultFD() { if (fd >= 0) close(fd); }

    int get() const { return fd; }

private:
    int fd;
};
