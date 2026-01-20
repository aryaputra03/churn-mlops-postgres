set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo "Docker Compose Operations"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Select operation:"
echo "  1) Start all services"
echo "  2) Run training only"
echo "  3) Run preprocessing only"
echo "  4) Run evaluation only"
echo "  5) Run complete pipeline"
echo "  6) Run DVC pipeline"
echo "  7) Start Jupyter notebook (dev)"
echo "  8) Stop all services"
echo "  9) Clean up (remove containers + volumes)"
echo "  0) Build images"
echo ""

read -p "Enter choice [0-9]: " choice

case $choice in
    1)
        echo -e "${GREEN}Starting all services...${NC}"
        docker-compose -f docker/docker-compose.yml up
        ;;
    2)
        echo -e "${GREEN}Running training...${NC}"
        docker-compose -f docker/docker-compose.yml up ml-training
        ;;
    3)
        echo -e "${GREEN}Running preprocessing...${NC}"
        docker-compose -f docker/docker-compose.yml up ml-preprocess
        ;;
    4)
        echo -e "${GREEN}Running evaluation...${NC}"
        docker-compose -f docker/docker-compose.yml up ml-evaluate
        ;;
    5)
        echo -e "${GREEN}Running complete pipeline...${NC}"
        docker-compose -f docker/docker-compose.yml up ml-pipeline-full
        ;;
    6)
        echo -e "${GREEN}Running DVC pipeline...${NC}"
        docker-compose -f docker/docker-compose.yml up dvc-pipeline
        ;;
    7)
        echo -e "${GREEN}Starting jupyter notebook...${NC}"
        docker-compose -f docker/docker-compose.yml up ml-dev
        echo ""
        echo -e "${YELLOW}Jupyter available at: http://localhost:8888${NC}"
        ;;
    8)
        echo -e "${YELLOW}Stopping all services...${NC}"
        docker-compose -f docker/docker-compose.yml down
        echo -e "${GREEN}Services stopped${NC}"
        ;;
    9)
        echo -e "${YELLOW}Cleaning up...${NC}"
        docker-compose -f docker/docker-compose.yml down-v
        docker system prune -f
        echo -e "${GREEN}Cleanup complete${NC}"
        ;;
    0)
        echo -e "${GREEN}Building images...${NC}"
        docker-compose -f docker/docker-compose.yml build
        echo -e "${GREEN}Build complete${NC}"
        ;;
    *)
        echo -e "${RED}Invalid option${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}Done!${NC}"

