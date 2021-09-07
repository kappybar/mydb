#include "db.hpp"

Page DiskManager::fetch_page(int pageid) {
    assert(pageid < page_num);
    file_stream.seekg(pageid * PAGESIZE,std::ios_base::beg);
    char page[PAGESIZE];
    file_stream.read(page,PAGESIZE);
    assert(!file_stream.fail());
    return Page(pageid,page);
}
    
void DiskManager::write_page(int pageid,Page &page) {
    assert(pageid < page_num);
    if (page.dirty) {
        file_stream.seekg(pageid * PAGESIZE,std::ios_base::beg);
        file_stream.write(page.page,PAGESIZE);
        page.dirty = false;
    }
}

void DiskManager::flush(void) {
    if (file_stream.sync() == -1) {
        error("DiskManager::flush");
    }
}

int DiskManager::allocate_new_page(void) {
    int pageid = page_num;
    ++page_num;
    if (truncate(file_name.c_str(),page_num * PAGESIZE) == -1) {
        error("truncate(allocate_new_page)");
    }
    return pageid;
}