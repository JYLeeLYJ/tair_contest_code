#ifndef LRU_CACHE_INCLUDE_H
#define LRU_CACHE_INCLUDE_H

#include <vector>
#include <unordered_map>
#include <memory>
#include "utils.hpp"

template <class K, class T>
struct Node{
    K key;
    T data;
    Node *prev, *next;
};

template <class K, class T , std::size_t size>
class alignas(CACHELINE_SIZE) lru_cache{
public:
    lru_cache(){
        auto p = new Node<K,T>[size];
        entries_.reset(p);
        hashmap_.reserve(size);
        free_entries_.resize(size);
        for(uint i=0; i<size; ++i)
            free_entries_[i] = p + i;
        head_.prev = nullptr;
        head_.next = &tail_;
        tail_.prev = &head_;
        tail_.next = nullptr;

    }

    void put(K key, T && data){
        auto *node = hashmap_[key];
        if(node){ // node exists
            detach(node);
            node->data = std::move(data);
            attach(node);
        }
        else{
            if(free_entries_.empty()){
                node = tail_.prev;
                detach(node);
                hashmap_.erase(node->key);
            }
            else{
                node = free_entries_.back();
                free_entries_.pop_back();
            }
            node->key = key;
            node->data = std::move(data);
            hashmap_[key] = node;
            attach(node);
        }
    }

    T * get(K key){
        // auto *node = hashmap_[key];
        auto p = hashmap_.find(key);
        if(node){
            detach(node);
            attach(node);
            return &node->data;
        }
        else{
            return nullptr;
        }
    }

    std::size_t free_space(){
        return free_entries_.size();
    }
private:
    void detach(Node<K,T>* node){
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }
    void attach(Node<K,T>* node){
        node->prev = &head_;
        node->next = head_.next;
        head_.next = node;
        node->next->prev = node;
    }
private:
    std::unordered_map<K, Node<K,T>* > hashmap_;
    std::vector<Node<K,T>* > free_entries_; 
    Node<K,T> head_, tail_;
    std::unique_ptr<Node<K,T>[]> entries_; 
};

#endif