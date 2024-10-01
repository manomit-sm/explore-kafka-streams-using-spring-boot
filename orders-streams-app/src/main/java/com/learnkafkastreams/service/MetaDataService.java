package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.HostInfoDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class MetaDataService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public List<HostInfoDTO> getStreamsMetaData() {
       return Objects.requireNonNull(streamsBuilderFactoryBean
                        .getKafkaStreams())
                .metadataForAllStreamsClients()
                .stream()
                .map(streamsMetadata -> new HostInfoDTO(streamsMetadata.hostInfo().host(),streamsMetadata.hostInfo().port()))
               .collect(Collectors.toList());
    }
}
