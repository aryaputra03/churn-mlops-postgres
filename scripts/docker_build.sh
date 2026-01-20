set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

IMAGE_NAME='churn-classifier'
IMAGE_TAG='latest'
DEV_TAG='dev'
REGISTRY=""

echo -e "${BLUE}========================================"
echo "Docker Build Script"
echo -e "========================================${NC}"
echo ""

BUILD_TYPE="prod"
NON_CACHE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --dev)
            BUILD_TYPE="dev"
            shift
            ;;
        --non-cache)
            NO_CACHE="--non-cache"
            shift
            ;;
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        --tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ "$BUILD_TYPE" == "prod" ] || [ "$BUILD_TYPE" == "all" ]; then
    echo -e "${GREEN}Building Production Image${NC}"
    echo "  Image: ${IMAGE_NAME}:${IMAGE_TAG}"
    echo "  Dockerfile: docker/Dockerfile"
    echo ""

    docker build \
        ${NO_CACHE} \
        -t ${IMAGE_NAME}:${IMAGE_TAG} \
        -f docker/Dockerfile \
        .
    
    if [ $? -eq 0 ]; then 
        echo -e "${GREEN}Production image built successfully${NC}"

        if [ -n "$REGISTRY" ]; then
            docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}
            echo -e "${GREEN}Tagged as ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}${NC}"
        fi
    else
        echo -e "${RED}Production build failed${NC}"
        exit 1
    fi
    echo ""
fi

if [ "$BUILD_TYPE" == "dev" ] || [ "$BUILD_TYPE" == "all" ]; then
    echo -e "${GREEN}Building Development Image${NC}"
    echo "  Image: ${IMAGE_NAME}:${DEV_TAG}"
    echo "  Dockerfile: docker/Dockerfile.dev"
    echo ""

    docker build \
        ${NO_CACHE} \
        -t ${IMAGE_NAME}:${DEV_TAG} \
        -f docker/Dockerfile.dev
        .
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Development image built successfully${NC}"

        if [ -n "$REGISTRY" ]; then
            docker tag ${IMAGE_NAME}:${DEV_TAG} ${REGISTRY}/${IMAGE_NAME}:${DEV_TAG}
            echo -e "${GREEN}Tagged as ${REGISTRY}/${IMAGE_NAME}:${DEV_TAG}${NC}"
        fi
    else
        echo -e "${RED}Development build failed${NC}"
        exit 1
    fi
    echo ""
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Built Images:${NC}"

docker images | grep ${IMAGE_NAME}
echo ""

read -p "Do you want to run the container? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo -e "${YELLOW}Running container...${NC}"

    if [ "$BUILD_TYPE" == "dev" ]; then
        docker run -it --rm \
            -v $(pwd):/app \
            ${IMAGE_NAME}:${DEV_TAG} \
            bash
    else
        docker run --rm \
            -v $(pwd)/data:/app/data \
            -v $(pwd)/models:/app/models \
            -v $(pwd)/params.yml:/app/params.yml \
            ${IMAGE_NAME}:${IMAGE_TAG}
    fi
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Build Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Usage examples:"
echo "  Production: docker run --rm -v \$(pwd)/data:/app/data ${IMAGE_NAME}:${IMAGE_TAG}"
echo "  Development: docker run -it --rm -v \$(pwd):/app ${IMAGE_NAME}:${DEV_TAG} bash"
echo "  Jupyter: docker run -p 8888:8888 ${IMAGE_NAME}:${DEV_TAG} jupyter notebook --ip=0.0.0.0 --allow-root"
if [ -n "$REGISTRY" ]; then
    echo ""
    echo "Push to registry:"
    echo "  docker push ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
    echo "  docker push ${REGISTRY}/${IMAGE_NAME}:${DEV_TAG}"
fi
echo ""