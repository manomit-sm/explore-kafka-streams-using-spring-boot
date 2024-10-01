package com.learnkafkastreams.controllers;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.service.MetaDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/metadata")
@RequiredArgsConstructor
public class MetaDataController {
    private final MetaDataService metaDataService;

    @GetMapping("/all")
    public List<HostInfoDTO> getStreamMetaData() {
        return metaDataService.getStreamsMetaData();
    }
}
