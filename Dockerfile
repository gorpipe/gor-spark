FROM registry.gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark:2.0.7

USER root
ENTRYPOINT ["/usr/bin/entrypoint.sh"]