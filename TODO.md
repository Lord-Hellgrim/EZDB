COMPLETE
- Implement compression     COMPLETE!!
    - Look into brotli maybe?

NEED POLISH
- Finish the full implementation of EZQL
    - Parser done. Testing and optimization to come.
    - Spec is half written (EZQL.txt)
- Re examine the ColumnTable
    - Try to further optimize queries
- Make documentation comments
- Reexamine the networking layer.
    - Find the Heisenbug
        - FOUND! The write lock that was updating the metadata was never releasing for some odd deadlock reason.
        I replaced the lock with an AtomicU64 and now the problem is gone.


IN PROGRESS
- Make a management GUI
    - Ideally a TUI to allow ssh GUI management and visualization
        - Note to self: "Look into the ratatui library"


NOT STARTED
- Make the tests more organized and stable
- Make a standard benchmark suite
- Implement database integrity guarantee
    - Oh boy...
- Further reinforce persistence
- Implement logging
- JAVASCRIPT!!!!
- Fix auth


- ...suggestions?