```mermaid
flowchart LR
    subgraph "OTel-Arrow Pipeline"
        subgraph "Receivers"
            otlp[OTLP]
            arrow[OTAP]
            custom_receiver[...]
        end

        subgraph "Processors" 
		    sample[Sample]
            transform[Transform]
            custom_processor[...]
        end

        subgraph "Exporters"
            otlp_exporter[OTLP]
            arrow_exporter[OTAP]
            custom_exporter[...]
        end

        Receivers }--> |Arrow Record Batches| Processors
        Processors -->{ |Arrow Record Batches| Exporters
    end

    subgraph "Telemetry"
        logs[Logs]
        metrics[Metrics]
        traces[Traces]
    end

    subgraph "Destinations"
        backends[Storage/Analysis Backends]
        vendors[Observability Vendors]
        custom_destination[...]
    end

    Telemetry --> Receivers
    Exporters --> Destinations
```


    classDef blue fill:#326ce5,stroke:#fff,stroke-width:1px,color:#fff;
    classDef lightblue fill:#4285f4,stroke:#fff,stroke-width:1px,color:#fff;
    classDef green fill:#34a853,stroke:#fff,stroke-width:1px,color:#fff;
    
    class Receivers,otlp,arrow,custom_receiver blue;
    class Processors,batch,memory_limiter,custom_processor lightblue;
    class Exporters,otlp_exporter,arrow_exporter,custom_exporter green;
