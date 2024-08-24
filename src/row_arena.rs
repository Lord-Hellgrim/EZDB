use fnv::FnvHashSet;



pub struct RowArena {
    buffer: Vec<u8>,
    row_size: usize,
    free_list: FnvHashSet<usize>,
    pointer: usize,
}

impl RowArena {

    pub fn new(row_size: usize) -> Self {
        RowArena {
            buffer: Vec::new(),
            row_size: row_size,
            free_list: FnvHashSet::with_hasher(fnv::FnvBuildHasher::default()),
            pointer: 0,
        }
    }

    pub fn allocate_rows_at_end(&mut self, number_of_rows: usize) -> usize {
        let returned_pointer = self.pointer;

        self.pointer += number_of_rows*self.row_size;

        returned_pointer
    }

    pub fn free_all(&mut self) {
        self.pointer = 0;
        self.buffer.shrink_to_fit();
    }

}