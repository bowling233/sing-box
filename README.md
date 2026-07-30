bowling233 的 sing-box fork：

- 上游：`https://github.com/SagerNet/sing-box` 的 `stable` 分支
- 维护：当上游发布新 tag 时，将本 fork 的变更 rebase 到上游的最新 tag 上
- 变更：
  - 原生 OpenTelemetry 流量上报
  - 仅发布 `linux/amd64`、`linux/arm64` GHCR 容器镜像

# sing-box

The universal proxy platform.

[![Packaging status](https://repology.org/badge/vertical-allrepos/sing-box.svg)](https://repology.org/project/sing-box/versions)

## Documentation

https://sing-box.sagernet.org

## License

```
Copyright (C) 2022 by nekohasekai <contact-sagernet@sekai.icu>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

In addition, no derivative work may use the name or imply association
with this application without prior consent.
```
