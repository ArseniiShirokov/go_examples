package lrucache

type Node struct {
	key, value int
	prev, next *Node
}

type LRU struct {
	capacity   int
	head, tail *Node
	itemsMap   map[int]*Node
}

func (l *LRU) Get(key int) (int, bool) {
	node, ok := l.itemsMap[key]
	if ok {
		l.moveToFront(node)
		return node.value, true
	}
	return 0, false
}

func (l *LRU) Set(key, value int) {
	if l.capacity == 0 {
		return
	}
	node, ok := l.itemsMap[key]
	if ok {
		node.value = value
		node.key = key
		l.moveToFront(node)
		return
	}

	if l.capacity == len(l.itemsMap) {
		delete(l.itemsMap, l.tail.key)
		l.moveToFront(l.tail)
		l.itemsMap[key] = l.head
		l.head.key = key
		l.head.value = value
	} else {
		node = l.pushFront(key, value)
		l.itemsMap[key] = node
	}
}

func (l *LRU) Range(f func(key, value int) bool) {
	cur := l.tail
	ans := true
	for cur != nil && ans {
		key := cur.key
		value := cur.value
		ans = f(key, value)
		cur = cur.prev
	}
}

func (l *LRU) Clear() {
	l.head = nil
	l.tail = nil
	for key := range l.itemsMap {
		//node.prev, node.next, node = nil, nil, nil
		delete(l.itemsMap, key)
	}
}

func (l *LRU) moveToFront(node *Node) {
	if node == l.head {
		return
	}
	if node == l.tail {
		l.tail = l.tail.prev
		l.tail.next = nil
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}
	l.head.prev = node
	node.next = l.head
	l.head = l.head.prev
	l.head.prev = nil
}

func (l *LRU) pushFront(key, value int) *Node {
	node := &Node{
		key:   key,
		value: value,
	}
	if l.head == nil {
		l.tail = node
		l.head = node
	} else {
		l.head.prev = node
		node.next = l.head
		l.head = node
	}
	return node
}

func New(cap int) Cache {
	return &LRU{cap, nil, nil, make(map[int]*Node, cap)}
}
