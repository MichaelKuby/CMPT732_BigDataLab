from pyspark import SparkConf, SparkContext
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def parse_input(string):
    tokens = string.split(':')
    key = int(tokens[0])
    values = tokens[1]
    nodes = values.split(' ')
    nodes = [int(node) for node in nodes if node]
    return key, nodes


def new_nodes(record):
    k, v = record
    list_of_accessible_nodes, (source, distance) = v
    for node in list_of_accessible_nodes:
        yield node, (k, distance + 1)


def find_shortest_path(v1, v2):
    source1, distance1 = v1
    source2, distance2 = v2
    if distance1 < distance2:
        return v1
    else:
        return v2


def longest_paths(record, current_longest_path):
    k, v = record
    path_list, (source, distance) = v
    return distance == current_longest_path


def main(inputs, output, source, destination):
    graph = sc.textFile(inputs + "/links-simple-sorted.txt")

    graph = graph.map(parse_input)
    graph.cache()

    found = False   # To identify whether a path was found or not

    # Create an RDD depicting our starting point
    starting_point = [(source, (None, 0))]
    shortest_paths = sc.parallelize(starting_point)
    fringe = shortest_paths

    for i in range(6):

        # Join nodes on the fringe to the graph to identify new shortest paths.
        print("fringe before join: ", fringe.take(5)) # format (node, (edgeList), (source, distance))
        fringe = graph.join(fringe)
        print("fringe after join with graphs: ", fringe.take(5))  # format (node, (edgeList), (source, distance))
        fringe.cache()


        # Find the new shortest paths to fringe nodes
        new_shortest_paths = fringe.flatMap(new_nodes)
        print("new paths from fringe: ", new_shortest_paths.take(5))

        # Update the fringe
        fringe = new_shortest_paths

        # Update our list of shortest paths
        all_shortest_paths = shortest_paths.union(new_shortest_paths)
        shortest_paths = all_shortest_paths.reduceByKey(lambda x, y: find_shortest_path(x, y))
        print("shortest paths: ", shortest_paths.take(5), "\n")
        shortest_paths.cache()

        # Save output
        shortest_paths.saveAsTextFile(output + '/iter-' + str(i))
        if shortest_paths.filter(lambda kv: kv[0] == destination).count() > 0:
            found = True
            break


    current = destination
    path = []
    if found:
        while True:
            path.append(current)
            # There will be only one record at this point, so collect is appropriate.
            list_kv = shortest_paths.filter(lambda kv: kv[0] == current).collect()
            s, d = list_kv[0][1]
            current = s
            if current is None:
                break

    path.reverse()
    final_path = sc.parallelize(path)
    final_path.saveAsTextFile(output + '/path')


if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    destination = int(sys.argv[4])
    main(inputs, output, source, destination)