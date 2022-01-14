FROM registry.gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark:2.0.2

USER root
ENTRYPOINT ["/usr/bin/entrypoint.sh"]