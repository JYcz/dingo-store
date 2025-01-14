#/usr/bin/env python3

from os.path import dirname, abspath
import argparse
import time

import dingosdk

dir = dirname(dirname(dirname(abspath(__file__))))

parser = argparse.ArgumentParser(description='argparse')
parser.add_argument('--coordinator_url', '-url', type=str, default ="file://"+ dir+"/bin/coor_list", help="coordinator url, try to use like file://./coor_list")
args = parser.parse_args()

g_schema_id = 2
g_index_id = 0
g_index_name = "example01"
g_range_partition_seperator_ids = [5, 10, 20]
g_dimension = 2
g_flat_param = dingosdk.FlatParam(g_dimension, dingosdk.kL2)
g_vector_ids = []

s, g_client = dingosdk.Client.Build(args.coordinator_url)
assert s.ok(), f"client build fail, {s.ToString()}"

s, g_vector_client = g_client.NewVectorClient()
assert s.ok(), f"dingo vector client build fail, {s.ToString()}"

def prepare_vector_index():
    global g_index_id
    s, creator = g_client.NewVectorIndexCreator()
    assert s.ok(),  f"dingo creator build fail: {s.ToString()}"

    creator.SetSchemaId(g_schema_id)
    creator.SetName(g_index_name)
    creator.SetReplicaNum(3)
    creator.SetRangePartitions(g_range_partition_seperator_ids)
    creator.SetFlatParam(g_flat_param)
    s, g_index_id = creator.Create()
    print(f"create index status: {s.ToString()}, index_id: {g_index_id}")
    assert s.ok(), f"create index fail: {s.ToString()}"

    time.sleep(20)

def post_clean(use_index_name=False):
    if use_index_name:
        tmp, index_id = g_client.GetIndexId(g_schema_id, g_index_name)
        print(f"index_id: {index_id}, g_index_id: {g_index_id}, get indexid: {tmp.ToString()}")
        assert index_id == g_index_id
        tmp = g_client.DropIndexByName(g_schema_id, g_index_name)
    else:
        tmp = g_client.DropIndex(g_index_id)

    print(f"drop index status: {tmp.ToString()}, index_id: {g_index_id}")
    g_vector_ids.clear()

def vector_add(use_index_name=False):
    vectors = []

    delta = 0.1
    for id in g_range_partition_seperator_ids:
        tmp_vector = dingosdk.Vector(dingosdk.kFloat, g_dimension)
        tmp_vector.float_values = [1.0 + delta, 2.0 + delta]
        tmp = dingosdk.VectorWithId(id, tmp_vector)
        vectors.append(tmp)

        g_vector_ids.append(id)
        delta += 1

    if use_index_name:
        add = g_vector_client.AddByIndexName(g_schema_id, g_index_name, vectors, False, False)
    else:
        add = g_vector_client.AddByIndexId(g_index_id, vectors, False, False)

    print(f"add vector status: {add.ToString()}")

def vector_search(use_index_name=False):
    target_vectors = []
    init = 0.1
    for i in range(5):
        tmp_vector = dingosdk.Vector(dingosdk.kFloat, g_dimension)
        tmp_vector.float_values = [init, init]

        tmp = dingosdk.VectorWithId()
        tmp.vector = tmp_vector
        target_vectors.append(tmp)

        init = init + 0.1

    param = dingosdk.SearchParameter()
    param.topk = 2
    # param.use_brute_force = True
    param.extra_params[dingosdk.kParallelOnQueries] = 10

    if use_index_name:
        tmp, result = g_vector_client.SearchByIndexName(g_schema_id, g_index_name, param, target_vectors)
    else:
        tmp, result = g_vector_client.SearchByIndexId(g_index_id, param, target_vectors)

    print(f"vector search status: {tmp.ToString()}")
    for r in result:
        print(f"vector search result: {dingosdk.DumpToString(r)}")

    assert len(result) == len(target_vectors)

    for i in range(len(result)):
        search_result = result[i]
        if search_result.vector_datas:
            assert len(search_result.vector_datas) == param.topk
        vector_id = search_result.id
        assert vector_id.id == target_vectors[i].id
        assert vector_id.vector.Size() == target_vectors[i].vector.Size()

def vector_delete(use_index_name=False):
    if use_index_name:
        tmp, result = g_vector_client.DeleteByIndexName(g_schema_id, g_index_name, g_vector_ids)
    else:
        tmp, result= g_vector_client.DeleteByIndexId(g_index_id, g_vector_ids)

    print(f"vector delete status: {tmp.ToString()}")
    for r in result:
        print(f"vector delete result: {dingosdk.DumpToString(r)}")

    for i in range(len(result)):
        delete_result = result[i]
        print(f"vector_id: {delete_result.vector_id}, bool is deleted: {delete_result.deleted}")

if __name__ == "__main__":
    prepare_vector_index()
    vector_add()
    vector_search()
    vector_delete()
    post_clean()

    prepare_vector_index()
    vector_add(True)
    vector_search(True)
    vector_delete(True)
    post_clean(True)

