#include "db.hpp"

// must release memory after read 
const char *Page::read(int offset,int len) {
    assert(offset + len <= PAGESIZE);
    access = 1;
    char *buf = static_cast<char*>(calloc(len+1,sizeof(char)));
    std::copy(page+offset,page+offset+len,buf);
    return buf;
}

void Page::write(const char buf[],int offset,int len) {
    assert(checksum_len <= offset && offset + len <= PAGESIZE);
    dirty = true;
    access = 1;
    std::copy(buf,buf+len,page+offset);
}

void Page::update_checksum(void) {
    unsigned int checksum = crc32(page + checksum_len,PAGESIZE - checksum_len);
    std::string checksum_str = to_hex(checksum);
    dirty = true;
    std::copy(checksum_str.begin(),checksum_str.end(),page);
}

bool Page::confirm_checksum(void) {
    const char *buf = read(0,checksum_len);
    unsigned int checksum = strtol(buf,NULL,16);
    unsigned int right_checksum = crc32(page + checksum_len,PAGESIZE - checksum_len);
    free(const_cast<char*>(buf));
    return checksum == right_checksum;
}

void Page::pin(void) {
    ++pin_count;
}

void Page::unpin(void) {
    --pin_count;
}