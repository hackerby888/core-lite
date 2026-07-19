#pragma once

#include <string>

// Load normal or ONLY_LOGGING settings before applying the test layout.
#include "private_settings.h"

#undef LOG_BUFFER_PAGE_SIZE
#undef PMAP_LOG_PAGE_SIZE
#undef IMAP_LOG_PAGE_SIZE
#undef VM_NUM_CACHE_PAGE

#define LOG_BUFFER_PAGE_SIZE 10000000ULL
#define PMAP_LOG_PAGE_SIZE 1000000ULL
#define IMAP_LOG_PAGE_SIZE 300ULL
#define VM_NUM_CACHE_PAGE 1
