#include "db.hpp"

const int MAX_BUFFER_SIZE = 1000;


void BufferManager::fetch_page(int pageid) {
    if (pagetable.count(pageid) > 0) {
        return;
    }
    if (pagetable.size() >= MAX_BUFFER_SIZE) {
        int page_index = evict();
        pages[page_index] = disk_manager.fetch_page(pageid);
        pagetable[pageid] = page_index;
    } else {
        int page_index = pagetable.size();
        pages[page_index] = disk_manager.fetch_page(pageid);
        pagetable[pageid] = page_index;
    }
}

int BufferManager::create_new_page(void) {
    return disk_manager.allocate_new_page();
}

const char *BufferManager::read_page(int pageid,int offset,int len) {
    fetch_page(pageid);
    assert(pagetable.count(pageid) > 0);
    int page_index = pagetable[pageid];
    return pages[page_index].read(offset,len);
}

void BufferManager::write_page(int pageid,const char buf[],int offset,int len) {
    fetch_page(pageid);
    assert(pagetable.count(pageid) > 0);
    int page_index = pagetable[pageid];
    pages[page_index].write(buf,offset,len);
}

void BufferManager::evict_page(int pageid) {
    assert(pagetable.count(pageid) > 0);
    int index = pagetable[pageid];
    pages[index].update_checksum();
    disk_manager.write_page(pageid,pages[index]);
    pagetable.erase(pageid);
}

void BufferManager::flush(void) {
    for(auto [pageid,_] : pagetable) {
        evict_page(pageid);
        (void)_;
    }
    disk_manager.flush();
}

int BufferManager::evict(void) {
    for(int i = 0;i < 2 * (int)pages.size(); i++) {
        int victim_index = (victim_index_base + i) % pages.size();
        if (pages[victim_index].access == 0 && pages[victim_index].pin_count == 0) {
            evict_page(pages[victim_index].pageid);
            victim_index_base = (victim_index + 1) % pages.size();
            return victim_index;
        }
        pages[victim_index].access = 0;
    }
    // we cannot evict page
    assert(false);
}