#!/bin/sh


dummy_slurm_executables() {
    tmp=$(mktemp -d)
    cat > $tmp/sbatch << EOF
#!/bin/sh
id="\$RANDOM"
{
echo \$(basename "\$0") "\$@" "::" "\$id"
}>$tmp/\$(date +"%s%3N")\$id.txt
echo "\$id"
EOF
    chmod +x $tmp/sbatch
    echo "$tmp"
}

path=$(dummy_slurm_executables)
trap "rm -rf --one-file-system $path" EXIT SIGINT

export PATH="$path:$PATH"
guile -L "$PWD" "$@"
cat "$path/"*.txt
