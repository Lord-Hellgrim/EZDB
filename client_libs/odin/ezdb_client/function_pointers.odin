package ezdb_client


add_ints :: proc (x, y: int) -> int { 
    return x + y 
}

// Taking a proc as an argument works just like the local variable above
combine_ints :: proc (x, y: int, procedure: proc (int, int) -> int) -> int {
return procedure(x, y)
}


fp_demo :: proc() {

    
    // #type does nothing and is just a visual indicator that what follows is a proc pointer (not a proc definition)
    // Parameter and return names are optional
    // op: #type proc (a, b: int) -> (c: int)

    // identical to the above
    op: proc (int, int) -> int
    
    op = add_ints // just use the proc's name to get its pointer
    assert(op(1, 2) == 3) // just call the pointer like it's a proc name
    
    // you can also declare a proc inline, it's basically the same syntax as a "normal" declaration but not using ::
    op = proc (x, y: int) -> int { return x*y }
    assert(op(1, 2) == 2)
    
    assert(combine_ints(2, 3, add_ints) == 5)
    
    //As proc pointers are just pointers (like I said, like Rust's basic `fn` pointers, not the `Fn` traits), 
    //they *can't* capture local variables from the enclosing scope. 
    //You'd need to *explicitly* pass them in somehow (via a proc argument *or* smuggle it in via the `context`)
    
    z := 3
    // op = proc (x, y: int) -> int { return x*y*z } // not allowed

    context.user_index = z
    op = proc (x, y: int) -> int { return x*y*context.user_index } // works--but remember it's passed in via the context/call stack, not the proc pointer

}