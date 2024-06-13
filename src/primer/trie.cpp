#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

using namespace std;

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto root = root_;

  if (root == nullptr) {
    return nullptr;
  }

  if (key.empty()) {
    auto valued_node = dynamic_cast<const TrieNodeWithValue<T> *>(root.get());
    if (!valued_node) {
      return nullptr;
    }
    return valued_node->value_.get();
  }

  auto cur = root;
  for (auto c : key) {
    if (cur->children_.find(c) == cur->children_.end()) {
      return nullptr;
    }
    cur = cur->children_.at(c);
  }

  if (cur->is_value_node_) {
    auto valued_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
    if (!valued_node) {
      return nullptr;
    }
    return valued_node->value_.get();
  }

  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (key.empty()) {
    auto value_ptr = make_shared<T>(std::move(value));
    shared_ptr<TrieNodeWithValue<T>> new_root = nullptr;
    if (root_->children_.empty()) {
      new_root = make_shared<TrieNodeWithValue<T>>(value_ptr);
    } else {
      new_root = make_shared<TrieNodeWithValue<T>>(root_->children_, value_ptr);
    }
    return Trie(new_root);
  }

  shared_ptr<TrieNode> new_root = nullptr;
  if (!root_) {
    new_root = make_shared<TrieNode>();
  } else {
    new_root = root_->Clone();
  }

  std::function<void(shared_ptr<TrieNode>, string_view)> put = [&value, &put](shared_ptr<TrieNode> r, string_view key) {
    bool find = false;
    for (auto &[c, n] : r->children_) {
      if (key.at(0) == c) {
        find = true;
        if (key.size() > 1) {
          shared_ptr<TrieNode> next = n->Clone();
          put(next, key.substr(1));
          n = next;
        } else {
          auto new_node = make_shared<TrieNodeWithValue<T>>(n->children_, make_shared<T>(std::move(value)));
          n = new_node;
        }
        return;
      }
    }

    if (!find) {
      char c = key.at(0);
      if (key.size() == 1) {
        r->children_.insert({c, make_shared<TrieNodeWithValue<T>>(make_shared<T>(std::move(value)))});
      } else {
        auto next = make_shared<TrieNode>();
        put(next, key.substr(1));
        r->children_.insert({c, next});
      }
    }
  };

  put(new_root, key);

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (root_ == nullptr) {
    return *this;
  }

  if (key.empty()) {
    if (root_->is_value_node_) {
      if (root_->children_.empty()) {
        return {};
      } else {
        auto new_root = make_shared<TrieNode>(root_->children_);
        return Trie{new_root};
      }
    }
    return *this;
  }

  shared_ptr<TrieNode> new_root = root_->Clone();

  std::function<bool(shared_ptr<TrieNode>, string_view)> remove = [&remove](shared_ptr<TrieNode> r, string_view key) {
    for (auto &[k, v] : r->children_) {
      if (k != key.at(0)) {
        continue;
      }

      if (key.size() == 1) {
        if (!v->is_value_node_) {
          return false;
        }

        if (v->children_.empty()) {
          r->children_.erase(k);
        } else {
          v = make_shared<TrieNode>(v->children_);
        }
        return true;
      }

      shared_ptr<TrieNode> next = v->Clone();
      if (!remove(next, key.substr(1))) {
        return false;
      }

      if (next->children_.empty() && !next->is_value_node_) {
        r->children_.erase(k);
      } else {
        v = next;
      }
      return true;
    }
    return false;
  };

  if (!remove(new_root, key)) {
    return *this;
  }

  if (new_root->children_.empty() && !new_root->is_value_node_) {
    new_root = nullptr;
  }

  return Trie{new_root};
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
