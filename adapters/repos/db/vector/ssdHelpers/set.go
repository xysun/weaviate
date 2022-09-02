package ssdhelpers

import (
	"context"
)

type Set struct {
	items       *Node
	vectorForID VectorForID
	distance    DistanceFunction
	center      []float32
	capacity    int
	size        int
	lastNode    *Node
	firstNode   *Node
}

type Node struct {
	data   IndexAndDistance
	left   *Node
	right  *Node
	parent *Node
	height int
}

type IndexAndDistance struct {
	index    uint64
	distance float32
	visited  bool
}

func NewSet(capacity int, vectorForID VectorForID, distance DistanceFunction, center []float32) *Set {
	return &Set{
		items:       nil,
		vectorForID: vectorForID,
		distance:    distance,
		center:      center,
		capacity:    capacity,
		size:        0,
		lastNode:    nil,
		firstNode:   nil,
	}
}

func (s *Set) Add(x uint64) *Set {
	vec, _ := s.vectorForID(context.Background(), x)
	dist := s.distance(vec, s.center)
	data := IndexAndDistance{
		index:    x,
		distance: dist,
		visited:  false,
	}

	//first element in the tree
	if s.items == nil {
		s.size++
		s.items = &Node{
			left:   nil,
			right:  nil,
			parent: nil,
			data:   data,
			height: 0,
		}
		s.lastNode = s.items
		s.firstNode = s.items
		return s
	}

	var last *Node = nil
	if s.size == s.capacity {
		//element to add too big so it will not get in
		if s.lastNode.data.distance < dist {
			return s
		}
		last = s.lastNode
	}

	if s.items.Add(data, s) {
		//already there, no need to add anything
		if last == nil {
			s.size++
			return s
		}
		//element added so the last needs out
		if last.parent == nil {
			if s.items.right != nil {
				if s.items.left != nil {
					s.items.left.parent = s.items.right
				}
				s.items.right.left = s.items.left
				s.items.right.Balance(s)

				if s.items.right != nil {
					s.items.right.parent = nil
				}
				s.items = s.items.right
				s.items.Balance(s)
				return s
			}

			if s.items.left != nil {
				s.items.left.parent = nil
			}
			s.items = s.items.left
			s.lastNode = s.items.Last()
			return s
		}
		//new element was not added to the right of the last
		if last.right == nil {
			if last.left != nil {
				last.left.parent = last.parent
			}
			last.parent.right = last.left
			last.parent.Balance(s)
			if last.parent.right == nil {
				s.lastNode = last.parent
				return s
			}
			s.lastNode = last.parent.right.Last()
			return s
		}
		//new element was added to the right of the last
		if last.right != nil {
			last.right.parent = last.parent
		}
		last.parent.right = last.right
		last.parent.Balance(s)
		if last.left != nil {
			last.left.parent = last.right
		}
		last.right.left = last.left
		last.right.Balance(s)
		s.lastNode = last.parent.right
		return s
	}
	return s
}

func (n *Node) Add(data IndexAndDistance, s *Set) bool {
	if n.data.index == data.index {
		return false
	}
	if n.data.distance > data.distance {
		if n.left == nil {
			n.left = &Node{
				left:   nil,
				right:  nil,
				data:   data,
				parent: n,
				height: 0,
			}
			if s.firstNode.data.distance > data.distance {
				s.firstNode = n.left
			}
			n.Balance(s)
			return true
		}
		return n.left.Add(data, s)
	}
	if n.right == nil {
		n.right = &Node{
			left:   nil,
			right:  nil,
			data:   data,
			parent: n,
			height: 0,
		}
		if s.firstNode.data.distance > data.distance {
			s.firstNode = n.right
		}
		if s.lastNode == n {
			s.lastNode = n.right
		}
		n.Balance(s)
		return true
	}
	return n.right.Add(data, s)
}

const (
	NOTBALANCE   int = 0
	BALANCERIGHT     = 1
	BALANCELEFT      = 2
)

func (n *Node) maxHeight() (int, int) {
	if n.right != nil {
		if n.left != nil {
			if n.right.height < n.left.height {

				if n.left.height-n.right.height > 1 {
					return n.right.height, BALANCERIGHT
				}
				return n.right.height, NOTBALANCE
			}
			if n.right.height-n.left.height > 1 {
				return n.left.height, BALANCELEFT
			}
			return n.left.height, NOTBALANCE

		}
		return n.right.height, NOTBALANCE

	}
	if n.left != nil {
		return n.left.height, NOTBALANCE
	}
	return 0, NOTBALANCE
}

func (n *Node) Balance(s *Set) {
	newHeight, action := n.maxHeight()

	if newHeight == n.height {
		return
	}

	if action == BALANCELEFT {
		n.BalanceLeft(s)
	} else if action == BALANCERIGHT {
		n.BalanceRight(s)
	}

	if n.parent != nil {
		n.parent.Balance(s)
	}
}

func (n *Node) BalanceLeft(s *Set) {
	if n.parent != nil {
		parent := n.parent
		right := n.right
		if n.parent.right == n {
			parent.right = right
			n.right = parent.right.left
			parent.right.left = n
		} else {
			parent.left = right
			n.right = parent.left.left
			parent.left.left = n
		}
		n.height, _ = n.maxHeight()
		right.height, _ = right.maxHeight()

		right.parent = parent
		n.parent = right
		if n.right != nil {
			n.right.parent = n
		}
		return
	}
	right := n.right
	if n.parent.right == n {
		s.items.right = right
		n.right = s.items.right.left
		s.items.right.left = n
	} else {
		s.items.left = right
		n.right = s.items.left.left
		s.items.left.left = n
	}
	n.height, _ = n.maxHeight()
	right.height, _ = right.maxHeight()

	right.parent = nil
	n.parent = right
	if n.right != nil {
		n.right.parent = n
	}
}

func (n *Node) BalanceRight(s *Set) {
	if n.parent != nil {
		parent := n.parent
		left := n.left
		if n.parent.right == n {
			parent.right = left
			n.left = parent.right.left
			parent.right.left = n
		} else {
			parent.left = left
			n.left = parent.left.left
			parent.left.left = n
		}
		n.height, _ = n.maxHeight()
		left.height, _ = left.maxHeight()

		left.parent = parent
		n.parent = left
		if n.left != nil {
			n.left.parent = n
		}
		return
	}
	left := n.left
	if n.parent.right == n {
		s.items.right = left
		n.left = s.items.right.left
		s.items.right.left = n
	} else {
		s.items.left = left
		n.left = s.items.left.left
		s.items.left.left = n
	}
	n.height, _ = n.maxHeight()
	left.height, _ = left.maxHeight()

	left.parent = nil
	n.parent = left
	if n.right != nil {
		n.right.parent = n
	}
}

func (n *Node) Last() *Node {
	if n.right == nil {
		return n
	}
	return n.right.Last()
}

func (s *Set) NotVisited() bool {
	return !s.firstNode.data.visited
}

func (n *Node) NotVisited() bool {
	return !n.data.visited || (n.left != nil && n.left.NotVisited()) || (n.right != nil && n.right.NotVisited())
}

func (s *Set) AddRange(indices []uint64) *Set {
	for _, item := range indices {
		s.Add(item)
	}
	return s
}

func (s *Set) Size() int {
	return s.size
}

func (s *Set) Top() uint64 {
	x := s.firstNode.data.index
	s.firstNode.data.visited = true
	s.firstNode.BackwardsTop(s)
	return x
}

func (n *Node) BackwardsTop(s *Set) {
	if !n.data.visited {
		s.firstNode = n
		return
	}

	if n.right != nil {
		if n.right.Top(s) {
			return
		}
	}

	if n.parent != nil {
		n.parent.BackwardsTop(s)
	}
}

func (n *Node) Top(s *Set) bool {
	if n.left != nil {
		if n.left.Top(s) {
			return true
		}
	}

	if !n.data.visited {
		s.firstNode = n
		return true
	}

	if n.right != nil {
		return n.right.Top(s)
	}

	return false
}

func (s *Set) Elements(k int) []uint64 {
	res := make([]uint64, s.size)
	i := s.items.Elements(res, 0)
	if k < i {
		i = k
	}
	return res[:i]
}

func (n *Node) Elements(buffer []uint64, offset int) int {
	if n.left != nil {
		offset = n.left.Elements(buffer, offset)
	}
	buffer[offset] = n.data.index
	offset++
	if n.right != nil {
		offset = n.right.Elements(buffer, offset)
	}
	return offset
}
