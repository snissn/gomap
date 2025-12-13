package adaptive

import (
	"github.com/snissn/gomap/TreeDB/internal/page"
	"github.com/snissn/gomap/TreeDB/internal/pager"
)

// ComputeLeafStats scans the B+Tree rooted at root and returns:
// - average leaf fill fraction (0..1) based on free space
// - total leaf count
func ComputeLeafStats(p *pager.Pager, root page.PageID) (float64, uint64, error) {
	if p == nil || root == 0 {
		return 0, 0, nil
	}

	var (
		stack     = []page.PageID{root}
		leafCount uint64
		fillSum   float64
	)

	for len(stack) > 0 {
		n := len(stack) - 1
		pid := stack[n]
		stack = stack[:n]

		buf, err := p.ReadPage(pid)
		if err != nil {
			return 0, 0, err
		}
		h, body, err := page.SplitPage(buf)
		if err != nil {
			return 0, 0, err
		}

		switch h.Flags {
		case page.PageTypeLeaf:
			lp, err := page.OpenLeafPage(buf)
			if err != nil {
				return 0, 0, err
			}
			free := lp.FreeSpace()
			if free < 0 {
				free = 0
			}
			fill := float64(len(body)-free) / float64(len(body))
			if fill < 0 {
				fill = 0
			}
			if fill > 1 {
				fill = 1
			}
			fillSum += fill
			leafCount++

		case page.PageTypeInternal:
			ip, err := page.OpenInternalPage(buf)
			if err != nil {
				return 0, 0, err
			}
			count := ip.Count()
			for i := 0; i < count; i++ {
				_, child, err := ip.EntryAt(i)
				if err != nil {
					return 0, 0, err
				}
				stack = append(stack, child)
			}
		default:
			// Ignore other page types.
		}
	}

	if leafCount == 0 {
		return 0, 0, nil
	}
	return fillSum / float64(leafCount), leafCount, nil
}
