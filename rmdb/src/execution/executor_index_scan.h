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

#include <limits>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    std::vector<Rid> rids_;
    size_t pos_ = 0;
    bool is_end_ = true;

    SmManager *sm_manager_;

    void write_sentinel(char *dest, const ColMeta &col, bool high) {
        if (col.type == TYPE_INT) {
            int val = high ? std::numeric_limits<int>::max() : std::numeric_limits<int>::min();
            memcpy(dest, &val, sizeof(int));
        } else if (col.type == TYPE_FLOAT) {
            float val = high ? std::numeric_limits<float>::max() : -std::numeric_limits<float>::max();
            memcpy(dest, &val, sizeof(float));
        } else {
            memset(dest, high ? 0xff : 0x00, col.len);
        }
    }

    const char *condition_value_raw(Condition &cond, int len) {
        if (cond.rhs_val.raw == nullptr) {
            cond.rhs_val.init_raw(len);
        }
        return cond.rhs_val.raw->data;
    }

    void update_lower(char *dest, bool &has_lower, bool &inclusive, const char *value,
                      ColType type, int len, bool value_inclusive) {
        int cmp = has_lower ? compare_raw_value(value, dest, type, len) : 1;
        if (!has_lower || cmp > 0) {
            memcpy(dest, value, len);
            has_lower = true;
            inclusive = value_inclusive;
        } else if (cmp == 0) {
            inclusive = inclusive && value_inclusive;
        }
    }

    void update_upper(char *dest, bool &has_upper, bool &inclusive, const char *value,
                      ColType type, int len, bool value_inclusive) {
        int cmp = has_upper ? compare_raw_value(value, dest, type, len) : -1;
        if (!has_upper || cmp < 0) {
            memcpy(dest, value, len);
            has_upper = true;
            inclusive = value_inclusive;
        } else if (cmp == 0) {
            inclusive = inclusive && value_inclusive;
        }
    }

    void build_index_bounds(std::vector<char> &lower_key, bool &has_lower, bool &lower_inclusive,
                            std::vector<char> &upper_key, bool &has_upper, bool &upper_inclusive) {
        lower_key.assign(index_meta_.col_tot_len, 0);
        upper_key.assign(index_meta_.col_tot_len, 0);
        int offset = 0;
        for (auto &col : index_meta_.cols) {
            write_sentinel(lower_key.data() + offset, col, false);
            write_sentinel(upper_key.data() + offset, col, true);
            offset += col.len;
        }

        offset = 0;
        for (auto &col : index_meta_.cols) {
            Condition *eq_cond = nullptr;
            for (auto &cond : conds_) {
                if (cond.is_rhs_val && cond.lhs_col.tab_name == tab_name_ && cond.lhs_col.col_name == col.name &&
                    cond.op == OP_EQ) {
                    eq_cond = &cond;
                    break;
                }
            }

            if (eq_cond != nullptr) {
                const char *value = condition_value_raw(*eq_cond, col.len);
                memcpy(lower_key.data() + offset, value, col.len);
                memcpy(upper_key.data() + offset, value, col.len);
                has_lower = true;
                has_upper = true;
                lower_inclusive = true;
                upper_inclusive = true;
                offset += col.len;
                continue;
            }

            for (auto &cond : conds_) {
                if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name_ || cond.lhs_col.col_name != col.name) {
                    continue;
                }
                const char *value = condition_value_raw(cond, col.len);
                if (cond.op == OP_GT || cond.op == OP_GE) {
                    update_lower(lower_key.data() + offset, has_lower, lower_inclusive, value, col.type, col.len,
                                 cond.op == OP_GE);
                } else if (cond.op == OP_LT || cond.op == OP_LE) {
                    update_upper(upper_key.data() + offset, has_upper, upper_inclusive, value, col.type, col.len,
                                 cond.op == OP_LE);
                }
            }
            break;
        }
    }

    void advance_to_valid_record() {
        while (pos_ < rids_.size()) {
            rid_ = rids_[pos_];
            auto record = fh_->get_record(rid_, context_);
            if (eval_conditions(cols_, *record, fed_conds_)) {
                is_end_ = false;
                return;
            }
            ++pos_;
        }
        is_end_ = true;
    }

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        std::vector<char> lower_key;
        std::vector<char> upper_key;
        bool has_lower = false;
        bool has_upper = false;
        bool lower_inclusive = true;
        bool upper_inclusive = true;
        build_index_bounds(lower_key, has_lower, lower_inclusive, upper_key, has_upper, upper_inclusive);

        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
        rids_.clear();
        ih->scan_range(has_lower ? lower_key.data() : nullptr, has_lower, lower_inclusive,
                       has_upper ? upper_key.data() : nullptr, has_upper, upper_inclusive, &rids_);
        pos_ = 0;
        advance_to_valid_record();
    }

    void nextTuple() override {
        if (is_end_) {
            return;
        }
        ++pos_;
        advance_to_valid_record();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end_) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    bool is_end() const override { return is_end_; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return rid_; }
};
