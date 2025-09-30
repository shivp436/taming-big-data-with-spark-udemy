from pyspark import SparkConf, SparkContext
import sys

def convertToBFS(line, startCharacterID):
    """Convert raw line to BFS format: (heroID, (connections, distance, color))"""
    fields = line.split()
    heroID = int(fields[0])
    
    connections = []
    for conn in fields[1:]:  
        connections.append(int(conn))
    
    color = 'WHITE'
    distance = 9999
    
    if heroID == startCharacterID:
        color = 'GRAY'
        distance = 0
    
    return (heroID, (connections, distance, color))

def createStartingRDD(sc, startCharacterID):
    """Create initial RDD with BFS starting state"""
    ip = sc.textFile('../data/Marvel+Graph')
    return ip.map(lambda line: convertToBFS(line, startCharacterID))

def bfsMap(node, endCharacterID, hitCounter):
    """
    Explore neighbors of GRAY nodes
    Returns list of new nodes to process
    """
    characterID = node[0]
    data = node[1]
    connections, distance, color = data[0], data[1], data[2]
    
    results = []
    
    # If node needs to be explored (is GRAY)
    if color == 'GRAY':
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            
            # Check if we found the target
            if endCharacterID == newCharacterID:
                hitCounter.add(1)
            
            # Create new entry for this neighbor
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)
        
        # Mark current node as fully explored
        color = 'BLACK'
    
    # Always emit the current node to preserve it
    results.append((characterID, (connections, distance, color)))
    return results

def bfsReduce(path1, path2):
    """
    Combine data for same characterID
    - Preserve connections
    - Keep shortest distance
    - Keep darkest color (BLACK > GRAY > WHITE)
    """
    edges1, distance1, color1 = path1[0], path1[1], path1[2]
    edges2, distance2, color2 = path2[0], path2[1], path2[2]
    
    # Initialize with defaults
    edges = []
    distance = 9999
    color = 'WHITE'
    
    # Preserve the connection list (only one should have it)
    if len(edges1) > 0:
        edges = edges1
    elif len(edges2) > 0:
        edges = edges2
    
    # take minimum distance
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2
    
    # Take darkest color (most explored state)
    # BLACK > GRAY > WHITE
    if color1 == 'BLACK' or color2 == 'BLACK':
        color = 'BLACK'
    elif color1 == 'GRAY' or color2 == 'GRAY':
        color = 'GRAY'
    else:
        color = 'WHITE'
    
    return (edges, distance, color)

def main():
    if len(sys.argv) < 3:
        print("Usage: python filename.py startCharacterID endCharacterID")
        sys.exit(1)
    
    startCharacterID = int(sys.argv[1])
    endCharacterID = int(sys.argv[2])
    
    conf = SparkConf().setMaster('local').setAppName('SuperHero-BFS')
    sc = SparkContext(conf=conf)
    
    print("""
    === Marvel Superhero BFS Path Finder ===
    
    BFS (Breadth-First Search):
    - Explores graph level by level
    - Finds shortest path between two nodes
    - Uses color coding: WHITE (unvisited) -> GRAY (discovered) -> BLACK (explored)
    
    Starting search from Hero ID {} to Hero ID {}
    """.format(startCharacterID, endCharacterID))
    
    # Accumulator to detect when target is found
    hitCounter = sc.accumulator(0)
    
    # Create initial RDD with starting node marked as GRAY
    iterationRDD = createStartingRDD(sc, startCharacterID)
    
    # Run BFS for maximum 10 iterations
    for iteration in range(0, 10):
        print(f'\n=== Running BFS Iteration {iteration + 1} ===')
        
        mapped = iterationRDD.flatMap(lambda node: bfsMap(node, endCharacterID, hitCounter))
        
        # Trigger computation to update accumulator
        processedCount = mapped.count()
        print(f'Processing {processedCount} nodes')
        
        # Check if we found the target
        if hitCounter.value > 0:
            print(f'\n*** SUCCESS! ***')
            print(f'Found target character from {hitCounter.value} different path(s)')
            print(f'Shortest path distance: {iteration} degrees of separation')
            break
        
        # Reduce by key to combine data for each character
        iterationRDD = mapped.reduceByKey(bfsReduce)
    else:
        # Handle case where target not found
        print(f'\nTarget not found within {10} iterations')
        print('Characters may not be connected or require more iterations')
    
    # Cleanup
    sc.stop()

if __name__ == '__main__':
    main()
