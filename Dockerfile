FROM registry.gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark:2.0.5

USER root
ENTRYPOINT ["/usr/bin/entrypoint.sh"]