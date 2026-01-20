set -e

echo "========================================"
echo "DVC Setup"
echo "========================================"

if ! command -v dvc &> /dev/null; then
    echo "DVC is not installed. instally.."
    pip install dvc[s3]
else
    echo "DVC is already installed"

fi

if [ ! -d ".dvc"]; then
    echo "Initializing DVC..."
    dvc init
    git add .dvc .dvcignore
    git commit -m "Initialize DVC" || echo "Nothing to commit"
else 
    echo "DVC already initialize"
fi

echo ""
echo "Configure Remote Storage"
echo "----------------------------"
echo "Select storage type:"
echo "1) Local (/tmp/dvc-storage)"
echo "2) Amazon S3"
echo "3) Google Drive"
echo "4) Skip remote configuration"
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        echo "Setting up local storage...."
        mkdir -p /tmp/dvc-storage
        dvc remote add -d myremote /tmp/dvc-storage -f
        echo "Local remote configured: /tmp/dvc-storage"
        ;;
    2)
        read -p "Enter S3 bucket name: " bucket
        read -p "Enter S3 path (Optional): " s3path
        if [-z "$s3path"]; then
            dvc remote add -d myremote s3://${bucket}/dvc-storage -f
        else
            dvc remote add -d myremote s3://${bucket}/${s3path} -f
        fi
        echo "S3 remote configured"
        echo "Remember to configure AWS credentials!"
        ;;
    3)
        read -p "Enter Google Drive Folder ID: " gdrive_id
        dvc remote add -d myremote gdrive://${gdrive_id} -f
        echo "Google Drive remote configured"
        echo "Remember to authenticate with Google!"
        ;;
    4) 
        echo "Skipping remote configuration"
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

if [-n "$(git status --porcelain .dvc/config)"]; then
    git add .dvc/config
    git commit -m "Configure DVC remote storage" || echo "Nothing to commit"
fi

echo ""
echo "========================================"
echo "DVC Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Add data: dvc add data/raw/data.csv"
echo "  2. Commit: git add data/raw/data.csv.dvc .gitignore && git commit -m 'Add data'"
echo "  3. Push to remote: dvc push"
echo "  4. Pull from remote: dvc pull"
echo ""

