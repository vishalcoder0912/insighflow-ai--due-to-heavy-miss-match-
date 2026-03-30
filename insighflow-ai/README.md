# InsightFlow AI - Microservice Architecture

AI-powered data analytics platform with real-time insights, forecasting, and visualization.

## Architecture

```
insighflow-ai/
├── frontend/          # React + Vite + TypeScript + TailwindCSS
├── backend/          # FastAPI Python Backend
└── docker-compose.yml
```

## Services

### Frontend (Port 5173)
- React 18 with TypeScript
- Vite for fast development
- TailwindCSS + shadcn/ui components
- Recharts for data visualization
- React Query for API state management

### Backend (Port 8000)
- FastAPI with async support
- SQLAlchemy ORM with Alembic migrations
- JWT Authentication with RBAC
- Pandas + Scikit-learn for data analysis
- PostgreSQL database

## Quick Start

### Using Docker (Recommended)

```bash
docker compose up --build
```

### Manual Setup

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

**Backend:**
```bash
cd backend
python -m venv .venv
.venv\Scripts\activate  # Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
alembic upgrade head
uvicorn app.main:app --reload
```

## Environment Variables

### Backend (.env)
```env
DATABASE_URL=postgresql://user:password@localhost:5432/insighflow
SECRET_KEY=your-secret-key
REFRESH_SECRET_KEY=your-refresh-secret-key
```

### Frontend (.env)
```env
VITE_API_URL=http://localhost:8000/api/v1
```

## API Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Tech Stack

| Frontend | Backend |
|----------|---------|
| React 18 | FastAPI |
| TypeScript | Python 3.11+ |
| TailwindCSS | PostgreSQL |
| Vite | SQLAlchemy |
| Recharts | Pandas |
| shadcn/ui | Scikit-learn |

## License

MIT
