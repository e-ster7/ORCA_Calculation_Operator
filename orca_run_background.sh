#!/bin/bash
# ORCA Pipeline Background Execution Script
# For Linux/Mac

ACTION=$1

case "$ACTION" in
    start)
        echo "Starting ORCA pipeline in background..."
        nohup python main.py > pipeline_output.log 2>&1 &
        PID=$!
        echo $PID > pipeline.pid
        echo "Pipeline started with PID: $PID"
        echo "Log file: pipeline_output.log"
        ;;
    
    stop)
        if [ -f pipeline.pid ]; then
            PID=$(cat pipeline.pid)
            echo "Stopping pipeline (PID: $PID)..."
            kill $PID
            rm pipeline.pid
            echo "Pipeline stopped"
        else
            echo "No PID file found. Searching for python process..."
            pkill -f "python main.py"
            echo "Done"
        fi
        ;;
    
    status)
        if [ -f pipeline.pid ]; then
            PID=$(cat pipeline.pid)
            if ps -p $PID > /dev/null 2>&1; then
                echo "Pipeline is running (PID: $PID)"
                
                # Show queue status
                echo ""
                echo "Queue status:"
                tail -n 5 logs/pipeline_*.log | grep -E "(queued|completed|failed)"
            else
                echo "Pipeline not running (stale PID file)"
                rm pipeline.pid
            fi
        else
            if pgrep -f "python main.py" > /dev/null; then
                echo "Pipeline is running but no PID file found"
                pgrep -f "python main.py"
            else
                echo "Pipeline is not running"
            fi
        fi
        ;;
    
    log)
        echo "Showing last 20 lines of log (press Ctrl+C to exit)..."
        tail -f logs/pipeline_*.log
        ;;
    
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    
    *)
        echo "Usage: $0 {start|stop|status|log|restart}"
        echo ""
        echo "Commands:"
        echo "  start   - Start pipeline in background"
        echo "  stop    - Stop pipeline"
        echo "  status  - Check if pipeline is running"
        echo "  log     - View live log output"
        echo "  restart - Restart pipeline"
        exit 1
        ;;
esac
