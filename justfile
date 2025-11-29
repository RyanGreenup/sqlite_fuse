run:
    # cargo run 
    # Make a directory
    mkdir -p ~/Downloads/eg_fuse
    # Run on a sample sqlite file
    rm testing.sqlite  || true
    doas umount -l ~/Downloads/eg_fuse || true
    cargo run -- --user-id test_user --init-db ~/Downloads/eg_fuse/ testing.sqlite
