pub struct Permuter<T> {
    current: Vec<T>,
    done: bool,
    dir: Vec<bool>,
}

impl<T: Ord + Clone> Permuter<T> {
    pub fn new(mut list: Vec<T>) -> Permuter<T> {
        list.sort();
        Permuter {
            current: list.clone(),
            done: false,
            dir: vec![true; list.len()],
        }
    }

    pub fn has_next(&self) -> bool {
        !self.done
    }

    pub fn next(&mut self) -> Option<Vec<T>> {
        if self.done {
            return None;
        }

        let rval = self.current.clone();

        // get largest mobile element k
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

        match k {
            Some(k) => {
                let swap = if self.dir[k] { pos - 1 } else { pos + 1 };
                self.current.swap(pos, swap);

                // reverse the direction of all elements larger than k
                for i in 0..length {
                    if self.current[i] > self.current[k] {
                        self.dir[i] = !self.dir[i];
                    }
                }
            }
            None => self.done = true,
        }

        Some(rval)
    }
}
