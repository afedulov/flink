/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, ChangeDetectionStrategy, ElementRef, Input, ViewChild } from '@angular/core';
import { JobFlameGraphNodeInterface } from 'interfaces';
import { select } from 'd3-selection';
import { flamegraph} from 'd3-flame-graph';

@Component({
  selector: 'flink-flame-graph',
  templateUrl: './flame-graph.component.html',
  styleUrls: [],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class FlameGraphComponent {

  @ViewChild('flameGraphContainer') flameGraphContainer: ElementRef<Element>;
  @Input() data: JobFlameGraphNodeInterface;

  draw() {
    if (this.data) {
      const element = this.flameGraphContainer.nativeElement;
      select(element).datum(this.data).call(flamegraph().width(element.clientWidth));
    }
  }
}
