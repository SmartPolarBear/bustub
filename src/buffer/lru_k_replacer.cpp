//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

// using namespace std;
using std::scoped_lock;

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // Do nothing.
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  scoped_lock _(latch_);

  // If the replacer is empty, return false.
  if (curr_size_ == 0) {
    return false;
  }

  // Try to evict in history_list_ first.
  if (!history_list_.empty()) {
    for (auto it = history_list_.begin(); it != history_list_.end(); ++it) {
      if (node_store_.at(*it).is_evictable_) {
        *frame_id = *it;
        node_store_.erase(*it);
        history_list_.erase(it);
        curr_size_--;
        return true;
      }
    }
  }

  // If history_list_ is empty, evict in cache_list_ based on k_distance.
  if (!cache_list_.empty()) {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
      if (node_store_.at(*it).is_evictable_) {
        *frame_id = *it;
        node_store_.erase(*it);
        cache_list_.erase(it);
        curr_size_--;
        return true;
      }
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  scoped_lock _(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    return;
  }

  if (auto iter = node_store_.find(frame_id); iter == node_store_.end()) {
    history_list_.push_back(frame_id);
    node_store_.emplace(frame_id, LRUKNode(1, prev(history_list_.end()), true));
    curr_size_++;
  } else {
    auto &[_, node] = *iter;
    node.k_++;
    node.history_.push_back(current_timestamp_);

    if (node.k_ == k_) {
      history_list_.erase(node.iter_);
    } else if (node.k_ > k_) {
      iter->second.history_.pop_front();
      cache_list_.erase(node.iter_);
    }

    // update position in cache_list_
    if (node.k_ >= k_) {
      size_t k_timestamp = node.history_.front();
      auto it = std::find_if(cache_list_.begin(), cache_list_.end(),
                             [&](const auto &id) { return k_timestamp <= node_store_.at(id).history_.front(); });
      node.iter_ = cache_list_.insert(it, frame_id);
    }

    current_timestamp_++;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  scoped_lock _(latch_);

  if (auto iter = node_store_.find(frame_id); iter != node_store_.end()) {
    if (iter->second.is_evictable_ && !set_evictable) {
      iter->second.is_evictable_ = false;
      curr_size_--;
    } else if (!iter->second.is_evictable_ && set_evictable) {
      iter->second.is_evictable_ = true;
      curr_size_++;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  scoped_lock _(latch_);

  auto pair = node_store_.find(frame_id);
  if (pair == node_store_.end()) {
    return;
  }

  if (!pair->second.is_evictable_) {
    return;
  }

  if (pair->second.k_ < k_) {
    history_list_.erase(pair->second.iter_);
  } else {
    cache_list_.erase(pair->second.iter_);
  }

  node_store_.erase(pair);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
