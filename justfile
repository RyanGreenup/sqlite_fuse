run:
    cargo run 
    # Make a directory
    mkdir -p ~/Downloads/eg_fuse
    # Run on a sample sqlite file
    rm testing.sqlite && doas umount -l ~/Downloads/eg_fuse; cargo run -- --init-db ~/Downloads/eg_fuse/ testing.sqlite
