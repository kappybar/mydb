#include "db.hpp"

unsigned int crc32table[256];
unsigned int generation_polynomial = 0xedb88320;// 0x4c11db7
bool made_crc32table = false;

void make_crc32table() {
    // | 32 bit | | 8 bit | 
    // 00...    0 
    // をgeneration_polynomialで右から割り算していく(F_2上)
    for(unsigned int i = 0;i < 256; i++) {
        unsigned int c = i;
        for(int j = 0;j < 8; j++) {
            c = (c & 1) ? (generation_polynomial ^ (c >> 1)) : (c >> 1);
        }
        crc32table[i] = c;
    }

    made_crc32table = true;
}

unsigned int crc32(const std::string &s) {
    if (!made_crc32table) {
        make_crc32table();
    }
    // | 32bit | | s |
    // 00...   0
    // を0xedb88320で右から割り算していく(F_2上)
    int len = (int)s.size();
    unsigned int crc = 0xffffffffu; // 4byte bit reverse
    for(int i = 0;i < len; i++) {
        crc = crc32table[(crc ^ s[i]) & 0xff] ^ (crc >> 8);
    }
    return crc ^ 0xffffffffu;
}

unsigned int crc32(const char *s,int len) {
    if (!made_crc32table) {
        make_crc32table();
    }
    unsigned int crc = 0xffffffffu; // 4byte bit reverse
    for(int i = 0;i < len; i++) {
        crc = crc32table[(crc ^ s[i]) & 0xff] ^ (crc >> 8);
    }
    return crc ^ 0xffffffffu;
}

std::string to_hex(unsigned int number) {
    std::ostringstream sout;
    sout << std::hex << std::setfill('0') << std::setw(8) << number;
    return sout.str();
}

unsigned int from_hex(const std::string &s) {
    assert(s.size() == 8);
    unsigned int number;
    try {
        number = std::stol(s,nullptr,16);
    } catch (const std::invalid_argument& e) {
        error("stol");
    } catch (const std::out_of_range& e) {
        error("stol");
    }
    return number;
}

void file_sync(const std::string &file_name) {
    int fd = open(file_name.c_str(),O_WRONLY|O_APPEND);
    if (fd == -1) {
        error("open(data_file)");
    }
    if (fsync(fd) == -1) {
        error("fsync(data_file)");
    } 
    if (close(fd) == -1) {
        error("close(data_file)");
    }
}

unsigned int file_size(const std::string &file_name) {
    int fd = open(file_name.c_str(),O_RDONLY);
    if (fd == -1) {
        error("open(file_size)");
    }
    struct stat buf;
    fstat(fd, &buf);
    off_t size = buf.st_size;
    if (close(fd) == -1) {
        error("close(file_size)");
    }
    return size;
}

void error(const char *s) {
    perror(s);
    exit(1);
}