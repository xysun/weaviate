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

func absIsBigger(a int, b int) bool {
	return a > b || a < -b
}

func (n *Node) CheckBalance() bool {
	if n.left != nil {
		if !n.left.CheckBalance() {
			return false
		}
		if n.right != nil {
			if !n.right.CheckBalance() {
				return false
			}
			return !absIsBigger(n.left.height-n.right.height, 1)
		}
		return n.height < 2
	}
	if n.right != nil {
		if !n.right.CheckBalance() {
			return false
		}
		return n.height < 2
	}
	return true
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
		if s.lastNode.data.distance <= dist {
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

				if s.items.right != nil {
					s.items.right.parent = nil
				}
				s.items = s.items.right
				s.items.right.Balance(s)
				//s.items.Balance(s)
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
			if last.parent.right == nil {
				s.lastNode = last.parent
				last.parent.Balance(s)
				return s
			}
			s.lastNode = last.parent.right.Last()
			last.parent.Balance(s)
			return s
		}
		//new element was added to the right of the last
		if last.right != nil {
			last.right.parent = last.parent
		}
		last.parent.right = last.right
		if last.left != nil {
			last.left.parent = last.right
		}
		last.right.left = last.left
		s.lastNode = last.parent.right
		last.parent.Balance(s)
		//last.right.Balance(s)
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
	NOTBALANCE     int = 0
	BALANCERIGHT       = 1
	BALANCELEFT        = 2
	BALANCEUPRIGHT     = 3
	BALANCEUPLEFT      = 4
)

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (n *Node) Height() int {
	if n == nil {
		return -1
	}
	return n.height
}

func (n *Node) maxHeight() (int, int) {
	height := max(n.left.Height(), n.right.Height()) + 1
	bal := n.right.Height() - n.left.Height()
	if bal > -2 && bal < 2 {
		return height, NOTBALANCE
	}
	if bal < 0 {
		bal = n.left.right.Height() - n.left.left.Height()
		if bal < 0 {
			return height, BALANCERIGHT
		}
		return height, BALANCEUPRIGHT
	}
	bal = n.right.right.Height() - n.right.left.Height()
	if bal > 0 {
		return height, BALANCELEFT
	}
	return height, BALANCEUPLEFT
}

func (n *Node) Balance(s *Set) {
	newHeight, action := n.maxHeight()

	if newHeight == n.height && action == NOTBALANCE {
		return
	}

	if action == BALANCELEFT {
		n.BalanceLeft(s)
	} else if action == BALANCERIGHT {
		n.BalanceRight(s)
	} else if action == BALANCEUPLEFT {
		n.BalanceUpLeft(s)
	} else if action == BALANCEUPRIGHT {
		n.BalanceUpRight(s)
	} else {
		n.height = newHeight
	}
	if n.parent != nil {
		n.parent.Balance(s)
	}

}

func (n *Node) BalanceLeft(s *Set) {
	right := n.right
	if n.parent != nil {
		parent := n.parent
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
	s.items = right
	n.right = s.items.left
	s.items.left = n
	n.height, _ = n.maxHeight()
	right.height, _ = right.maxHeight()

	right.parent = nil
	n.parent = right
	if n.right != nil {
		n.right.parent = n
	}
}

func (n *Node) BalanceUpLeft(s *Set) {
	right := n.right
	rleft := n.right.left
	right.left = rleft.right
	n.right = rleft.left
	if right.left != nil {
		right.left.parent = right
	}
	if n.right != nil {
		n.right.parent = n
	}
	rleft.left = n
	rleft.right = right

	if n.parent != nil {
		parent := n.parent
		if n.parent.right == n {
			parent.right = rleft
		} else {
			parent.left = rleft
		}
		n.height, _ = n.maxHeight()
		right.height, _ = right.maxHeight()
		rleft.height, _ = rleft.maxHeight()

		right.parent = rleft
		n.parent = rleft
		rleft.parent = parent
		return
	}
	s.items = rleft
	n.height, _ = n.maxHeight()
	right.height, _ = right.maxHeight()
	rleft.height, _ = rleft.maxHeight()

	right.parent = rleft
	n.parent = rleft
	rleft.parent = nil
}

func (n *Node) BalanceRight(s *Set) {
	left := n.left
	if n.parent != nil {
		parent := n.parent
		if n.parent.right == n {
			parent.right = left
			n.left = parent.right.right
			parent.right.right = n
		} else {
			parent.left = left
			n.left = parent.left.right
			parent.left.right = n
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
	s.items = left
	n.left = s.items.right
	s.items.right = n
	n.height, _ = n.maxHeight()
	left.height, _ = left.maxHeight()

	left.parent = nil
	n.parent = left
	if n.left != nil {
		n.left.parent = n
	}
}

func (n *Node) BalanceUpRight(s *Set) {
	left := n.left
	lright := n.left.right
	left.right = lright.left
	n.left = lright.right
	if left.right != nil {
		left.right.parent = left
	}
	if n.left != nil {
		n.left.parent = n
	}
	lright.right = n
	lright.left = left

	if n.parent != nil {
		parent := n.parent
		if n.parent.right == n {
			parent.right = lright
		} else {
			parent.left = lright
		}
		n.height, _ = n.maxHeight()
		left.height, _ = left.maxHeight()
		lright.height, _ = lright.maxHeight()

		left.parent = lright
		n.parent = lright
		lright.parent = parent
		return
	}
	s.items = lright
	n.height, _ = n.maxHeight()
	left.height, _ = left.maxHeight()
	lright.height, _ = lright.maxHeight()

	left.parent = lright
	n.parent = lright
	lright.parent = nil
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
		/*if !s.items.CheckBalance() {
			panic("")
		}*/
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
	res := make([]uint64, 0, s.size)
	res = s.items.Elements(res)
	if len(res) < k {
		k = len(res)
	}
	return res[:k]
}

func (n *Node) Elements(buffer []uint64) []uint64 {
	if n.left != nil {
		buffer = n.left.Elements(buffer)
	}
	buffer = append(buffer, n.data.index)
	if n.right != nil {
		buffer = n.right.Elements(buffer)
	}
	return buffer
}
