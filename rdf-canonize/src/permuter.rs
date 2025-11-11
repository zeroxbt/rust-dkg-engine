/// A `Permuter` struct for generating permutations of a list of elements.
/// This struct uses the Steinhaus-Johnson-Trotter algorithm to generate each permutation
/// by swapping elements in lexicographical order. It tracks direction and completion state
/// to efficiently produce the next permutation until all permutations have been generated.
///
/// # Type Parameters
/// * `T`: The type of the elements being permuted. Must implement `Ord` and `Clone`.
///
pub struct Permuter<T> {
    current: Vec<T>,
    done: bool,
    dir: Vec<bool>,
}

impl<T: Ord + Clone> Permuter<T> {
    /// Constructs a new `Permuter` with an initial list of elements.
    /// The list is sorted initially to start with the lexicographically smallest permutation.
    ///
    /// # Arguments
    /// * `list`: A vector of elements to permute.
    ///
    /// # Returns
    /// Returns a `Permuter` initialized to start generating permutations.
    pub fn new(mut list: Vec<T>) -> Permuter<T> {
        list.sort(); // Start with the smallest lexicographical permutation
        Permuter {
            current: list.clone(),
            done: false,
            dir: vec![true; list.len()], // Initially, all directions are set to left (true)
        }
    }

    /// Generates the next permutation of the list if available.
    ///
    /// # Returns
    /// Returns `Some(Vec<T>)` containing the next permutation if available, `None` if all permutations have been generated.
    pub fn next(&mut self) -> Option<Vec<T>> {
        if self.done {
            return None; // No more permutations to generate
        }

        let rval = self.current.clone(); // Clone the current permutation to return

        // Identify the largest mobile element (k) that can move according to its direction
        let mut k = None;
        let mut pos = 0;
        let length = self.current.len();
        for i in 0..length {
            let element = &self.current[i];
            let left = self.dir[i];
            let is_mobile = (left && i > 0 && element > &self.current[i - 1])
                || (!left && i < length - 1 && element > &self.current[i + 1]);

            if is_mobile && (k.is_none() || element > &self.current[k.unwrap()]) {
                k = Some(i);
                pos = i;
            }
        }

        // If a mobile element is found, perform the swap and adjust directions
        match k {
            Some(k) => {
                let swap = if self.dir[k] { pos - 1 } else { pos + 1 };
                self.current.swap(pos, swap); // Swap the elements

                // Reverse the direction of all elements larger than the current element at k
                for i in 0..length {
                    if self.current[i] > self.current[k] {
                        self.dir[i] = !self.dir[i];
                    }
                }
            }
            None => self.done = true, // If no mobile element is found, we're done
        }

        Some(rval) // Return the permutation generated before the swap
    }
}
