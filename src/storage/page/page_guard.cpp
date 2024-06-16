#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept : BasicPageGuard(that.bpm_, that.page_) {
  is_dirty_ = that.is_dirty_;
  that.CleanupState();
}

void BasicPageGuard::Drop() {
  if (HasValidState()) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  CleanupState();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    if (HasValidState()) {
      bpm_->UnpinPage(PageId(), is_dirty_);
    }

    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    that.CleanupState();
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {  // NOLINT
  Drop();
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.HasValidState()) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() {  // NOLINT
  Drop();
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.HasValidState()) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {  // NOLINT
  Drop();
}

}  // namespace bustub
