// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  
  // 创建 .ldb 文件，文件编号和 WAL 文件一样都是由 versions_->NewFileNumber() 产生的
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    // 记录下 imutable memtable 跳表中的最小的 internal key
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      // 从 level 0 遍历 imutable memtable 跳表
      key = iter->key();
      // iter->value() 实际上也是从 iter->key() 中解析出来的
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      // 记录下 imutable memtable 跳表中的最大的 internal key
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    // 按照SSTable格式写入 ldb 文件中，包含用户数据的 DataBlock 和辅助数据 FilterBlock / MetaIndex Block / IndexBlock 
    // 以及 footer
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      // sync 落盘
      s = file->Sync();
    }
    if (s.ok()) {
      // close fd
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      // 尝试把刚刚创建的 SSTable 加载到 TableCache 中并创建 iterator
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
