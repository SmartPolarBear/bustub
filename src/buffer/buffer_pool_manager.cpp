//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

using std::scoped_lock;

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  scoped_lock _{latch_};

  Page *p = nullptr;
  frame_id_t fid = -1;

  if (free_list_.empty()) {
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
    p = &pages_[fid];
  } else {
    p = &pages_[fid = free_list_.back()];
    free_list_.pop_back();
  }

  page_table_.erase(p->page_id_);

  if (p->IsDirty()) {
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
    p->is_dirty_ = false;
  }

  *page_id = AllocatePage();
  p->page_id_ = *page_id;
  page_table_[*page_id] = fid;
  p->pin_count_ = 1;
  p->ResetMemory();

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  return p;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  scoped_lock _{latch_};
  if (const auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    Page *p = &pages_[iter->second];
    p->pin_count_++;

    replacer_->RecordAccess(iter->second);
    replacer_->SetEvictable(iter->second, false);
    return p;
  }

  Page *p = nullptr;
  frame_id_t fid = -1;

  if (free_list_.empty()) {
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
    p = &pages_[fid];
  } else {
    p = &pages_[fid = free_list_.back()];
    free_list_.pop_back();
  }

  page_table_.erase(p->page_id_);

  if (p->IsDirty()) {
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
    p->is_dirty_ = false;
  }

  p->page_id_ = page_id;
  page_table_[page_id] = fid;
  p->pin_count_ = 1;
  p->ResetMemory();

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  disk_manager_->ReadPage(page_id, p->GetData());

  return p;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  scoped_lock _{latch_};
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    Page *p = &pages_[iter->second];
    p->is_dirty_ |= is_dirty;

    if (p->GetPinCount() <= 0) {
      return false;
    }
    p->pin_count_--;
    if (p->GetPinCount() == 0) {
      replacer_->SetEvictable(iter->second, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  scoped_lock _{latch_};
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  auto p = &pages_[iter->second];
  disk_manager_->WritePage(page_id, p->GetData());
  p->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  scoped_lock _{latch_};
  for (size_t i = 0; i < pool_size_; i++) {
    auto p = &pages_[i];
    if (p->GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
    p->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }

  scoped_lock _{latch_};
  const auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    DeallocatePage(page_id);
    return true;
  }

  const auto p = &pages_[iter->second];
  if (p->GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(page_id);
  free_list_.push_back(iter->second);
  replacer_->Remove(iter->second);

  p->ResetMemory();
  p->page_id_ = INVALID_PAGE_ID;
  p->is_dirty_ = false;
  p->pin_count_ = 0;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
