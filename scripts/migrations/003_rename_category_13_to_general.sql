-- Rename numeric category label to canonical non-numeric name.
-- Safe to run multiple times.
UPDATE categories
SET name = 'General'
WHERE id = 13
  AND name = '13';

