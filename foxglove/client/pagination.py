from typing import Callable, Generic, Iterator, List, Optional, TypeVar

T = TypeVar("T")


class Page(Generic[T]):
    """One server page. Use :meth:`auto_paging_iter` to traverse subsequent pages.

    ``items`` contains only this page. Cursors are opaque tokens that can be passed
    to the same list method with the same filters and ordering. ``limit`` on that
    method controls page size, not the total number of items yielded.
    """

    def __init__(
        self,
        items: List[T],
        *,
        next_cursor: Optional[str] = None,
        previous_cursor: Optional[str] = None,
        fetch_page: Callable[[str], "Page[T]"],
        legacy_offset: bool = False,
    ):
        self.items = items
        self.next_cursor = next_cursor
        self.previous_cursor = previous_cursor
        self._fetch_page = fetch_page
        self._legacy_offset = legacy_offset

    def auto_paging_iter(self) -> Iterator[T]:
        """Yield this page, then fetch further pages on demand in display order.

        Starting from a nonzero deprecated offset is unsupported: those responses
        do not contain continuation cursors. Request the first page or a cursor
        page instead. Errors fetching subsequent pages propagate to the caller.
        """
        if self._legacy_offset:
            raise ValueError(
                "Automatic pagination requires cursor pagination, not a nonzero offset"
            )
        page = self
        while True:
            yield from page.items
            if page.next_cursor is None:
                return
            page = page._fetch_page(page.next_cursor)
