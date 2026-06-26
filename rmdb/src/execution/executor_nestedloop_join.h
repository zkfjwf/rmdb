/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;
    std::unique_ptr<RmRecord> current_tuple_;
    std::vector<std::unique_ptr<RmRecord>> right_records_;
    int right_idx_ = -1;

    std::unique_ptr<RmRecord> make_join_record(const RmRecord &right_record) {
        auto left_record = left_->Next();
        if (left_record == nullptr) {
            return nullptr;
        }

        auto record = std::make_unique<RmRecord>(len_);
        memcpy(record->data, left_record->data, left_->tupleLen());
        memcpy(record->data + left_->tupleLen(), right_record.data, right_->tupleLen());
        return record;
    }

    void advance_to_valid_record() {
        current_tuple_ = nullptr;
        while (!left_->is_end() && !right_records_.empty()) {
            while (right_idx_ >= 0) {
                auto record = make_join_record(*right_records_[right_idx_]);
                if (record != nullptr && eval_conditions(cols_, *record, fed_conds_)) {
                    current_tuple_ = std::move(record);
                    isend = false;
                    return;
                }
                --right_idx_;
            }
            left_->nextTuple();
            if (!left_->is_end()) {
                right_idx_ = static_cast<int>(right_records_.size()) - 1;
            }
        }
        isend = true;
    }

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        right_records_.clear();
        for (; !right_->is_end(); right_->nextTuple()) {
            auto record = right_->Next();
            if (record != nullptr) {
                right_records_.push_back(std::move(record));
            }
        }
        right_idx_ = static_cast<int>(right_records_.size()) - 1;
        advance_to_valid_record();
    }

    void nextTuple() override {
        if (isend) {
            return;
        }
        --right_idx_;
        advance_to_valid_record();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (current_tuple_ == nullptr) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*current_tuple_);
    }

    bool is_end() const override { return isend; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return _abstract_rid; }
};
